/**
 * LV Frenzy — TikTok Live Bridge
 *
 * Šis process savienojas ar TikTok Live čatu (izmantojot tiktok-live-connector)
 * un pārsūta ziņas visiem pieslēgtajiem WebSocket klientiem (multichat lapai).
 *
 * Palaišana:
 *   1. npm install
 *   2. TIKTOK_USERNAME=tavstiktoks PORT=8081 node server.js
 *      (vai izveido .env failu ar šiem mainīgajiem)
 *
 * Ievieto TIKTOK_USERNAME bez @ zīmes, piem. "lvfrenzy".
 */

const { TikTokLiveConnection } = require('tiktok-live-connector');
const { WebSocketServer } = require('ws');

const TIKTOK_USERNAME = process.env.TIKTOK_USERNAME || '';
const PORT = process.env.PORT || 8081;
const RECONNECT_DELAY_MS = 15000;

if (!TIKTOK_USERNAME) {
  console.error('KĻŪDA: iestati TIKTOK_USERNAME vides mainīgo (bez @ zīmes).');
  process.exit(1);
}

// ---------- WebSocket serveris klientiem (multichat lapai) ----------
const wss = new WebSocketServer({ port: PORT });
console.log(`[bridge] WebSocket serveris klausās uz porta ${PORT}`);

const clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);
  console.log('[bridge] Jauns klients pieslēdzies. Kopā:', clients.size);
  broadcast({ type: 'status', text: 'Tilts aktīvs, gaida TikTok Live ziņas priekš @' + TIKTOK_USERNAME });

  ws.on('close', () => {
    clients.delete(ws);
    console.log('[bridge] Klients atvienojies. Kopā:', clients.size);
  });
});

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  for (const ws of clients) {
    if (ws.readyState === ws.OPEN) ws.send(msg);
  }
}

// ---------- TikTok Live savienojums ----------
let tiktokConnection = null;
let reconnectTimer = null;

function connectTikTok() {
  clearTimeout(reconnectTimer);

  tiktokConnection = new TikTokLiveConnection(TIKTOK_USERNAME, {});

  tiktokConnection.connect()
    .then((state) => {
      console.log(`[tiktok] Savienots ar @${TIKTOK_USERNAME}, room ID: ${state.roomId}`);
      broadcast({ type: 'status', text: 'Savienots ar TikTok Live: @' + TIKTOK_USERNAME });
    })
    .catch((err) => {
      console.error('[tiktok] Neizdevās savienoties (iespējams straume nav aktīva):', err.message || err);
      broadcast({ type: 'status', text: 'TikTok straume nav aktīva, mēģina vēlreiz...' });
      reconnectTimer = setTimeout(connectTikTok, RECONNECT_DELAY_MS);
    });

  tiktokConnection.on('chat', (data) => {
    console.log('[debug-chat]', JSON.stringify(data).slice(0, 500)); // pagaidu logs, lai redzētu īsto struktūru
    broadcast({
      type: 'chat',
      user: data.user?.nickname || data.user?.uniqueId || data.nickname || data.uniqueId || 'Nezināms',
      text: data.comment || data.content || data.text || data.message || '',
    });
  });

  tiktokConnection.on('gift', (data) => {
    console.log('[debug-gift]', JSON.stringify(data).slice(0, 500)); // pagaidu logs
    if (data.repeatEnd === false) return; // vēl turpinās virkne, gaidām beigas
    broadcast({
      type: 'gift',
      user: data.user?.nickname || data.user?.uniqueId || data.nickname || data.uniqueId || 'Nezināms',
      giftName: data.giftDetails?.giftName || data.giftDetails?.name || data.giftName || data.gift?.name || 'dāvana',
    });
  });

  tiktokConnection.on('streamEnd', () => {
    console.log('[tiktok] Straume beigusies, mēģinās savienoties vēlreiz vēlāk.');
    broadcast({ type: 'status', text: 'TikTok straume beigusies.' });
    reconnectTimer = setTimeout(connectTikTok, RECONNECT_DELAY_MS);
  });

  tiktokConnection.on('disconnected', () => {
    console.log('[tiktok] Atvienots, mēģinās savienoties vēlreiz.');
    reconnectTimer = setTimeout(connectTikTok, RECONNECT_DELAY_MS);
  });
}

connectTikTok();

process.on('SIGINT', () => {
  console.log('\n[bridge] Aizveras...');
  process.exit(0);
});
