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

// Se tiver uma lista de proxies separada por vírgula no .env
const PROXY_LIST = (process.env.SCRAPER_PROXY_LIST || process.env.SCRAPER_PROXY || '').split(',').filter(p => !!p);

function getBrowserlessWS() {
    // Usamos flags nativas do Browserless para aumentar o sucesso:
    // --stealth: Ativa o modo furtivo nativo do Browserless
    // --blockAds: Remove anúncios que podem causar lentidão e detecção
    // proxyCountry=br: Tenta usar infraestrutura brasileira se disponível
    let ws = `wss://chrome.browserless.io?token=${BROWSERLESS_TOKEN}&--stealth&--blockAds&proxyCountry=br`;

    if (PROXY_LIST.length > 0) {
        const randomProxy = PROXY_LIST[Math.floor(Math.random() * PROXY_LIST.length)];
        ws += `&--proxy-server=${randomProxy}`;
        console.log(`🛡️ [IP] Usando Proxy Externo: ${randomProxy.includes('@') ? randomProxy.split('@')[1] : randomProxy}`);
    } else {
        console.log('✨ [Info] Usando o "Stealth Mode" nativo do Browserless para evitar bloqueios.');
    }
    return ws;
}

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0 Safari/537.36'
];

function getRandomUA() {
    return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

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

    // Verificação de Banco na Partida
    try {
        const stats = await db.collection('listings').get();
        console.log(`📊 [Banco] Total de registros atuais no Firestore: ${stats.size}`);
        if (stats.size > 0) {
            console.log('Últimos 3 títulos cadastrados:');
            stats.docs.slice(-3).forEach(d => console.log(' - ' + (d.data().title || 'Sem Título')));
        }
    } catch (e) {
        console.error("❌ ERRO ao ler registros do banco:", e.message);
    }

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
        const wsUrl = getBrowserlessWS();
        console.log(`🌐 [Conexão] Iniciando nova sessão limpa...`);
        browser = await chromium.connectOverCDP(wsUrl);

        const context = await browser.newContext({
            userAgent: getRandomUA(),
            viewport: {
                width: 1280 + Math.floor(Math.random() * 100),
                height: 720 + Math.floor(Math.random() * 100)
            },
            locale: 'pt-BR',
            timezoneId: 'America/Sao_Paulo',
            geolocation: { longitude: -46.6333, latitude: -23.5505 },
            permissions: ['geolocation']
        });
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

                        // Detecção de Bloqueio / Cloudflare
                        const isBlocked = await page.evaluate(() => {
                            const t = document.title?.toLowerCase() || '';
                            const b = document.body?.innerText?.toLowerCase() || '';
                            return t.includes('blocked') || t.includes('captcha') || t.includes('access denied') || b.includes('sorry, you have been blocked');
                        });

                        if (isBlocked) {
                            console.warn(`🛑 Bloqueio detectado no anúncio! Pulando e ajustando tempo...`);
                            await updateStatus(`Bloqueado pelo OLX. Reajustando estratégia...`, currentProgress, adUrl, foundLinks);

                            // Em vez de reiniciar tudo (que gasta créditos), vamos apenas esperar mais
                            // e mudar o User Agent na próxima tentativa se possível via contexto, 
                            // mas aqui vamos apenas dar skip para não travar a fila.
                            const waitTime = 20000 + Math.floor(Math.random() * 30000);
                            await new Promise(r => setTimeout(r, waitTime));
                            continue;
                        }

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

                        if (data.title?.toLowerCase().includes("blocked") || data.title === "Sem Título") {
                            console.warn(`⚠️ Dados inválidos capturados (provável bloqueio): ${adUrl}`);
                            continue;
                        }

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
                        const verify = await db.collection('listings').doc(docId).get();
                        if (verify.exists) {
                            console.log(`✅ CONFIRMADO NO FIREBASE: ${data.title} - ${data.price} (ID: ${docId})`);
                        } else {
                            console.warn(`⁉️ ERRO: O registro de ${data.title} não foi encontrado após o salvamento.`);
                        }

                        results.push(item);
                        foundLinks.push(adUrl);

                        // Atualiza o status IMEDIATAMENTE após o save
                        await updateStatus(`Item ${results.length} salvo!`, Math.floor((results.length / limit) * 100), adUrl, foundLinks);

                        // Espera dinâmico e aleatório (Simula comportamento humano)
                        const nextWait = 15000 + Math.floor(Math.random() * 15000);
                        console.log(`😴 Aguardando ${Math.floor(nextWait / 1000)}s para o próximo...`);
                        await new Promise(r => setTimeout(r, nextWait));
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
