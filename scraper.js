require('dotenv').config();
const admin = require('firebase-admin');
const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
const { DateTime } = require('luxon');

// Add stealth plugin
chromium.use(stealth);

// ==========================================
// CONFIGURAÇÃO FIREBASE (ADMIN SDK)
// ==========================================
let serviceAccount;
try {
    if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    } else {
        // Fallback para arquivo local se no ambiente de desenvolvimento
        serviceAccount = require('./firebase-key.json');
    }
} catch (e) {
    console.error("❌ ERRO: Credenciais do Firebase ausentes.");
    process.exit(1);
}

if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount)
    });
}

const db = admin.firestore();
const BROWSERLESS_TOKEN = process.env.BROWSERLESS_TOKEN || '2U1KDYckXbPO4pc065f0e047f92f67e4ab2dbe8e65ac0fd55';
const BROWSERLESS_WS = `wss://chrome.browserless.io?token=${BROWSERLESS_TOKEN}`;

// ==========================================
// FUNÇÕES DE STATUS NO FIRESTORE
// ==========================================
async function updateStatus(message, progress = 0, currentItem = null, links = []) {
    console.log(`📡 [STATUS] ${message} (${progress}%)`);
    try {
        await db.collection('system').doc('status').set({
            message,
            progress,
            currentItem,
            links,
            lastUpdate: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
    } catch (e) {
        console.error("Erro ao atualizar status:", e.message);
    }
}

// ==========================================
// LÓGICA DE EXTRAÇÃO
// ==========================================
async function startScraping() {
    console.log('🚀 [Boot] Iniciando Scraper Autônomo...');

    // 1. Verificar Parâmetros na Base de Dados
    console.log('📡 Buscando parâmetros de extração (filtros)...');
    const filterDoc = await db.collection('system').doc('filters').get();

    if (!filterDoc.exists) {
        console.log('⚠️ [Fim] Nenhum parâmetro de extração (filtros) encontrado na base de dados.');
        console.log('Finalizado sem fazer a extração.');
        return;
    }

    const filters = filterDoc.data();
    if (!filters.regions || filters.regions.length === 0) {
        console.log('⚠️ [Fim] Filtros sem regiões selecionadas.');
        return;
    }

    console.log(`🎯 Parâmetros carregados: ${JSON.stringify(filters)}`);

    // Inicia status
    await updateStatus("Iniciando extração autônoma...", 5);

    try {
        const results = await performScrape(filters);
        console.log(`✅ Processo finalizado. Total processado: ${results.length}`);
        await updateStatus("Finalizado com sucesso!", 100);
    } catch (err) {
        console.error("💥 Erro crítico no processo:", err);
        await updateStatus(`Erro crítico: ${err.message}`, 0);
    } finally {
        // Encerrar processo para garantir que não fique rodando infinitamente se não houver agendamento interno
        process.exit(0);
    }
}

async function performScrape(filters) {
    const limit = filters.limit || 50;
    const foundLinks = [];
    const newResults = [];

    console.log(`📡 OLX: Conectando via Browserless...`);
    const browser = await chromium.connectOverCDP(BROWSERLESS_WS);
    const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    });
    const page = await context.newPage();
    await page.route('**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}', route => route.abort());

    // Gerar URLs baseado nos filtros
    const initialUrls = [];
    filters.regions.forEach(r => {
        filters.types.forEach(t => {
            initialUrls.push({
                url: generateOlxUrl(r, t, filters.priceMin, filters.priceMax),
                region: r,
                type: t
            });
        });
    });

    for (const urlObj of initialUrls) {
        if (newResults.length >= limit) break;

        console.log(`📡 OLX: Navegando para lista (${urlObj.region}/${urlObj.type}): ${urlObj.url}`);
        await page.goto(urlObj.url, { waitUntil: 'domcontentloaded', timeout: 60000 });

        const title = await page.title();
        if (title.includes("Access Denied") || title.includes("Cloudflare")) {
            console.error("🚫 Bloqueado pelo Cloudflare.");
            continue;
        }

        const adUrlsResults = await page.evaluate(() => {
            const cardLinks = Array.from(document.querySelectorAll('a.olx-adcard__link'));
            if (cardLinks.length > 0) return cardLinks.map(a => a.href.split('?')[0]);
            return Array.from(document.querySelectorAll('a'))
                .map(a => a.href)
                .filter(h => h && h.includes('olx.com.br/') && h.includes('/imoveis/') && /\d{8,}/.test(h))
                .map(h => h.split('?')[0]);
        });

        const targetUrls = Array.from(new Set(adUrlsResults)).slice(0, limit - newResults.length);

        for (const adUrl of targetUrls) {
            try {
                // Check if already exists to skip re-scraping the same item details
                const docId = Buffer.from(adUrl).toString('base64').replace(/[/+=]/g, '');
                const existing = await db.collection('listings').doc(docId).get();
                if (existing.exists && existing.data().status === 'ignored') continue;

                await updateStatus(`Extraindo ${newResults.length + 1}/${limit}`, 50, adUrl, foundLinks);
                await page.goto(adUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
                await page.waitForTimeout(1000);

                const data = await page.evaluate(() => {
                    const ogDesc = document.querySelector('meta[property="og:description"]')?.content || '';
                    const phoneRegex = /\(?\d{2}\)?\s*(?:9\s?\d{4}|[2-9]\d{3})[-\s]?\d{4}/g;
                    const phones = (ogDesc.match(phoneRegex) || []).filter(s => s.replace(/\D/g, '').length >= 10);
                    const bestPhone = phones.length > 0 ? phones[0] : "Não informado";

                    const getDetail = (text) => {
                        const sel = ['div[data-testid="ad-properties"] div', 'div.ad__sc-2h9gkk-0 div', '#details div'];
                        for (const s of sel) {
                            const found = Array.from(document.querySelectorAll(s)).find(el => el.innerText.includes(text));
                            if (found) {
                                const val = found.querySelector('a') || found.querySelector('span:last-child') || found;
                                return val.innerText.replace(text, "").replace(":", "").trim();
                            }
                        }
                        return null;
                    };

                    return {
                        title: document.querySelector('h1')?.innerText.trim() || "Sem Título",
                        price: document.querySelector('span.typo-display-large')?.innerText.trim() || "N/A",
                        phone: bestPhone,
                        contactName: ogDesc.match(/\(([^)]{2,30})\)\s*$/)?.[1]?.trim() || "Desconhecido",
                        rooms: getDetail("Quartos"),
                        area: getDetail("Área útil") || getDetail("Área construída") || getDetail("Área total"),
                        location: document.querySelector('.ad__sc-1m38784-0')?.innerText.replace("Exibir no mapa", "").trim() || "N/D"
                    };
                });

                // Gravando no Firebase
                const item = {
                    ...data,
                    link: adUrl,
                    region: urlObj.region,
                    listingType: urlObj.type,
                    status: existing.exists ? existing.data().status : "active",
                    capturedAt: existing.exists ? existing.data().capturedAt : admin.firestore.FieldValue.serverTimestamp(),
                    lastUpdated: admin.firestore.FieldValue.serverTimestamp()
                };

                await db.collection('listings').doc(docId).set(item, { merge: true });
                newResults.push(item);
                foundLinks.push(adUrl);
                console.log(`✅ Salvo: ${data.price} | ${adUrl}`);
            } catch (e) {
                console.error(`⚠️ Erro ao processar ${adUrl}: ${e.message}`);
            }
        }
    }
    await browser.close();
    return newResults;
}

function generateOlxUrl(region, type, priceMin, priceMax) {
    const regionMap = { 'alphaville': 'alphaville', 'tambore': 'tambore', 'barueri': 'barueri' };
    const regionPath = regionMap[region] || region;
    const baseUrl = `https://www.olx.com.br/imoveis/${type}/estado-sp/sao-paulo-e-regiao/${regionPath}`;
    const url = new URL(baseUrl);
    if (priceMin) url.searchParams.set('ps', priceMin);
    if (priceMax) url.searchParams.set('pe', priceMax);
    url.searchParams.set('f', 'p');
    url.searchParams.set('sf', '1');
    url.searchParams.set('sp', '6');
    return url.toString();
}

startScraping();
