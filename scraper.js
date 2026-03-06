require('dotenv').config();
const admin = require('firebase-admin');
const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();

chromium.use(stealth);

// ==========================================
// CONFIGURAÇÃO FIREBASE
// ==========================================
let serviceAccount;
try {
    serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT) : require('./firebase-key.json');
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

async function updateStatus(message, progress = 0, currentItem = null, links = []) {
    console.log(`📡 [Status] ${message} (${progress}%)`);
    await db.collection('system').doc('status').set({
        message, progress, currentItem, links,
        lastUpdate: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
}

// ==========================================
// MONITOR DE PEDIDOS (LOOP INFINITO)
// ==========================================
async function monitorRequests() {
    console.log('👀 [Monitor] Iniciado. Verificando pedidos a cada 30 segundos...');

    while (true) {
        try {
            // Verifica pedidos pendentes sem orderBy para evitar erro de índice
            const snapshot = await db.collection('requests')
                .where('status', '==', 'pending')
                .limit(10)
                .get();

            if (!snapshot.empty) {
                // Ordenar manualmente no JS
                const docs = snapshot.docs.sort((a, b) => {
                    const tA = a.data().requestedAt?.toDate() || 0;
                    const tB = b.data().requestedAt?.toDate() || 0;
                    return tA - tB;
                });

                const requestDoc = docs[0];
                const requestData = requestDoc.data();

                console.log(`🚀 [Execução] Processando pedido: ${requestDoc.id}`);

                // Marcar como processando para ninguém mais pegar
                await requestDoc.ref.update({ status: 'processing', startedAt: admin.firestore.FieldValue.serverTimestamp() });

                // Rodar Extração
                await performScrape(requestData.filters || {});

                // Finalizar e Deletar Pedido
                await requestDoc.ref.delete();
                console.log(`✅ [Fim] Pedido ${requestDoc.id} concluído e removido.`);

                // Aguarda um pouco antes de checar o próximo se acabou de rodar
                await new Promise(r => setTimeout(r, 5000));
                continue;
            } else {
                // console.log('😴 Nando pedido pendente...');
            }
        } catch (e) {
            console.error("💥 Erro no monitor:", e.message);
        }

        // Aguarda 30 segundos
        await new Promise(r => setTimeout(r, 30000));
    }
}

async function performScrape(filters) {
    const limit = filters.limit_enabled ? parseInt(filters.limit_value || 3) : 50;
    const results = [];
    const foundLinks = [];

    let browser;
    try {
        browser = await chromium.connectOverCDP(BROWSERLESS_WS);
        const context = await browser.newContext({ userAgent: 'Mozilla/5.0...' });
        const page = await context.newPage();
        await page.route('**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}', route => route.abort());

        await updateStatus("Robô conectado. Iniciando...", 10);

        const regions = filters.regions || ['alphaville'];
        const types = filters.types || ['venda'];

        for (const r of regions) {
            for (const t of types) {
                if (results.length >= limit) break;

                const url = generateOlxUrl(r, t, filters.priceMin, filters.priceMax);
                console.log(`📡 Navegando: ${url}`);

                try {
                    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });

                    const adLinks = await page.evaluate(() => {
                        const links = Array.from(document.querySelectorAll('a.olx-adcard__link')).map(a => a.href.split('?')[0]);
                        if (links.length > 0) return links;

                        // Fallback para outros tipos de cards da OLX
                        return Array.from(document.querySelectorAll('a'))
                            .map(a => a.href)
                            .filter(h => h && h.includes('olx.com.br/') && h.includes('/imoveis/') && /\d{8,}/.test(h))
                            .map(h => h.split('?')[0]);
                    });

                    for (const adUrl of adLinks.slice(0, limit - results.length)) {
                        console.log(`🔍 Analisando anúncio: ${adUrl}`);
                        const docId = Buffer.from(adUrl).toString('base64').replace(/[/+=]/g, '');
                        const existing = await db.collection('listings').doc(docId).get();

                        if (existing.exists && existing.data().status === 'ignored') {
                            console.log(`⏩ Ignorado (status no banco): ${adUrl}`);
                            continue;
                        }

                        // Calcula o progresso real baseado no limite
                        const currentProgress = Math.floor(((results.length) / limit) * 100);
                        await updateStatus(`Extraindo ${results.length + 1}/${limit}`, currentProgress, adUrl, foundLinks);

                        await page.goto(adUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

                        const data = await page.evaluate(() => {
                            const getDetail = (text) => {
                                const sel = [
                                    'div[data-testid="ad-properties"] div', // Olx TestId
                                    'div.ad__sc-2h9gkk-0 div',              // Nova Classe
                                    'ul.ad__sc-1f7p06-0 li',               // Lista de detalhes
                                    '#details div',                         // Fallback legado
                                    'div.sc-1f7p06-1'                       // Outra classe comum
                                ];
                                for (const s of sel) {
                                    const elements = Array.from(document.querySelectorAll(s));
                                    const found = elements.find(el => el.innerText.includes(text));
                                    if (found) {
                                        const valEl = found.querySelector('a') || found.querySelector('span:last-child') || found.querySelector('dt + dd') || found;
                                        if (valEl) {
                                            const rawValue = valEl.innerText.replace(text, "").replace(":", "").trim();
                                            if (rawValue && rawValue.length < 50) return rawValue;
                                        }
                                    }
                                }
                                return null;
                            };

                            const ogDesc = document.querySelector('meta[property="og:description"]')?.content || '';

                            return {
                                title: document.querySelector('h1')?.innerText?.trim() || "Sem Título",
                                price: document.querySelector('span.typo-display-large')?.innerText?.trim() || "N/A",
                                rooms: getDetail("Quartos"),
                                area: getDetail("Área útil") || getDetail("Área construída") || getDetail("Área total"),
                                garage: getDetail("Vagas na garagem") || getDetail("Vagas"),
                                condo: getDetail("Condomínio") || getDetail("Taxa de condomínio"),
                                location: document.querySelector('.ad__sc-1m38784-0')?.innerText?.replace("Exibir no mapa", "")?.trim() ||
                                    document.querySelector('span.sc-1f3m9u2-0')?.innerText?.trim() || "N/D",
                                contactName: ogDesc.match(/\(([^)]{2,30})\)\s*$/)?.[1]?.trim() || "Desconhecido"
                            };
                        });

                        const item = {
                            ...data,
                            link: adUrl,
                            region: r,
                            listingType: t,
                            status: existing.exists ? existing.data().status : 'active',
                            capturedAt: existing.exists ? existing.data().capturedAt : admin.firestore.FieldValue.serverTimestamp(),
                            lastUpdated: admin.firestore.FieldValue.serverTimestamp()
                        };

                        await db.collection('listings').doc(docId).set(item, { merge: true });
                        console.log(`✅ Extraído e Salvo: ${data.title} - ${data.price}`);

                        results.push(item);
                        foundLinks.push(adUrl);

                        // Atualiza o status IMEDIATAMENTE após o save para aparecer na tela original
                        await updateStatus(`Item ${results.length} salvo!`, Math.floor((results.length / limit) * 100), adUrl, foundLinks);

                        // Espera um pouco entre anúncios para evitar Cloudflare
                        await new Promise(r => setTimeout(r, 15000));
                    }
                } catch (e) {
                    console.error(`Erro na página ${url}:`, e.message);
                }
            }
        }
        await updateStatus("Extração concluída!", 100, null, foundLinks);
    } catch (err) {
        console.error("Erro Fatal:", err.message);
        await updateStatus(`Erro: ${err.message}`, 0);
    } finally {
        if (browser) await browser.close();
    }
}

function generateOlxUrl(region, type, min, max) {
    const url = new URL(`https://www.olx.com.br/imoveis/${type}/estado-sp/sao-paulo-e-regiao/${region}`);
    if (min) url.searchParams.set('ps', min);
    if (max) url.searchParams.set('pe', max);
    url.searchParams.set('f', 'p'); url.searchParams.set('sf', '1');
    return url.toString();
}

monitorRequests();
