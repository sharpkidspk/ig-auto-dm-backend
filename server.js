const http = require('http');
const crypto = require('crypto');
const db = require('./db');
const meta = require('./meta');

const VERSION = '1.5.0';
const PORT = Number(process.env.PORT || 10000);
const HOST = process.env.HOST || '0.0.0.0';
const WEBHOOK_VERIFY_TOKEN = process.env.WEBHOOK_VERIFY_TOKEN || '';
const TEST_FAST = process.env.TEST_FAST === '1';

function json(res, status, body) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
  });
  res.end(JSON.stringify(body));
}
function rawBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []; let size = 0;
    req.on('data', c => {
      size += c.length;
      if (size > 5_000_000) { reject(new Error('Request too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}
function parse(raw) {
  if (!raw.length) return {};
  try { return JSON.parse(raw.toString('utf8')); }
  catch { throw new Error('Invalid JSON'); }
}
function truthy(v) { return /^(1|true|yes|y|eligible|opted[-_ ]?in)$/i.test(String(v || '').trim()); }
function eligible(r) {
  return truthy(r.opt_in) && truthy(r.messaging_eligible === undefined ? 'yes' : r.messaging_eligible)
    && String(r.recipient_id || '').trim();
}
function template(text, row) {
  return String(text).replace(/\{\{\s*([^}]+?)\s*\}\}/g, (_, k) => row[k] ?? `{{${k}}}`);
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

const activationAttempts = new Map();
function activationAllowed(ip) {
  const now = Date.now(), windowMs = 15 * 60 * 1000, max = 10;
  const recent = (activationAttempts.get(ip) || []).filter(ts => now - ts < windowMs);
  if (recent.length >= max) { activationAttempts.set(ip, recent); return false; }
  recent.push(now); activationAttempts.set(ip, recent); return true;
}

let queueRunning = false;
let queuePaused = false;
let currentCampaign = null;

async function waitWhilePaused() {
  while (queuePaused && queueRunning) await sleep(TEST_FAST ? 10 : 500);
}
async function sleepPausable(ms) {
  let remaining = ms, step = TEST_FAST ? 5 : 250;
  while (remaining > 0 && queueRunning) {
    await waitWhilePaused();
    const slice = Math.min(step, remaining);
    await sleep(slice); remaining -= slice;
  }
}
async function persistCampaign(state) {
  if (!currentCampaign) return;
  await db.saveCampaign(currentCampaign, state || (queuePaused ? 'paused' : queueRunning ? 'running' : 'finished'));
}

async function runBulk(body, plan, rows) {
  queueRunning = true; queuePaused = false;
  currentCampaign = {
    id: crypto.randomUUID(), name: String(body.campaignName || 'Bulk Campaign'),
    ownerDeviceId: String(body.deviceId || ''), plan, accepted: rows.length,
    processed: 0, sent: 0, failed: 0, skipped: 0,
    currentRecipientId: '', currentUsername: '', startedAt: new Date().toISOString()
  };
  await persistCampaign('running');
  try {
    const deviceId = String(body.deviceId || '');
    const minDelay = TEST_FAST ? 0 : Math.max(10, Number(body.delayMin || 60));
    const maxDelay = TEST_FAST ? 0 : Math.max(minDelay, Number(body.delayMax || 120));
    const messageTemplate = String(body.template || '').trim();

    for (let i = 0; i < rows.length; i++) {
      await waitWhilePaused();
      const row = rows[i];
      const recipientId = String(row.recipient_id).trim();
      const message = body.personalized ? template(messageTemplate, row) : messageTemplate;
      currentCampaign.currentRecipientId = recipientId;
      currentCampaign.currentUsername = String(row.username || '');
      await persistCampaign();

      if (plan !== 'pro' && await db.usageFor(deviceId) >= 20) {
        currentCampaign.skipped++; currentCampaign.processed++;
        await db.addHistory({ type: 'bulk', recipientId, username: row.username || '', message,
          status: 'skipped: free daily quota reached', plan });
        await persistCampaign();
        continue;
      }

      try {
        const r = await meta.sendText(recipientId, message);
        currentCampaign.sent++;
        await db.addHistory({ type: 'bulk', recipientId, username: row.username || '', message,
          status: 'sent', messageId: r.message_id || '', plan });
        if (plan !== 'pro') await db.addUsage(deviceId, 1);
      } catch (e) {
        currentCampaign.failed++;
        await db.addHistory({ type: 'bulk', recipientId, username: row.username || '', message,
          status: 'failed: ' + e.message, plan });
      }
      currentCampaign.processed++;
      await persistCampaign();

      if (i < rows.length - 1 && maxDelay > 0) {
        const seconds = Math.floor(minDelay + Math.random() * (maxDelay - minDelay + 1));
        await sleepPausable(seconds * 1000);
      }
    }
  } finally {
    if (currentCampaign) {
      currentCampaign.currentRecipientId = '';
      currentCampaign.currentUsername = '';
      currentCampaign.finishedAt = new Date().toISOString();
      await persistCampaign('finished').catch(e => console.error('Campaign save error:', e.message));
    }
    queueRunning = false; queuePaused = false;
    setTimeout(() => { if (!queueRunning) currentCampaign = null; }, TEST_FAST ? 50 : 5000);
  }
}

async function handleWebhook(body) {
  if (!body || body.object !== 'instagram') return;
  const monitor = await db.getMonitor();
  for (const entry of body.entry || []) {
    for (const evt of entry.messaging || []) {
      if (evt.message?.is_echo) continue;
      const senderId = evt.sender?.id, text = evt.message?.text || '';
      await db.addHistory({ type: 'inbound', recipientId: senderId || '', message: text, status: 'received' });
      if (monitor.enabled && monitor.inbound && monitor.message && senderId) {
        const reply = template(monitor.message, { username: 'there' });
        try {
          const r = await meta.sendText(senderId, reply);
          await db.addHistory({ type: 'monitor-reply', recipientId: senderId, message: reply,
            status: 'sent', messageId: r.message_id || '' });
        } catch (e) {
          await db.addHistory({ type: 'monitor-reply', recipientId: senderId, message: reply,
            status: 'failed: ' + e.message });
        }
      }
    }

    const comments = [];
    if ((entry.field === 'comments' || entry.field === 'live_comments') && entry.value) comments.push(entry.value);
    for (const ch of entry.changes || []) {
      if ((ch.field === 'comments' || ch.field === 'live_comments') && ch.value) comments.push(ch.value);
    }
    for (const value of comments) {
      const commentId = value.id || value.comment_id;
      const username = value.from?.username || value.username || '';
      const text = value.text || '';
      await db.addHistory({ type: 'comment', recipientId: commentId || '', username, message: text, status: 'received' });
      if (monitor.enabled && monitor.comments && monitor.message && commentId) {
        const reply = template(monitor.message, { username });
        try {
          const r = await meta.sendPrivateReply(commentId, reply);
          await db.addHistory({ type: 'comment-private-reply', recipientId: r.recipient_id || commentId,
            username, message: reply, status: 'sent', messageId: r.message_id || '' });
        } catch (e) {
          await db.addHistory({ type: 'comment-private-reply', recipientId: commentId, username,
            message: reply, status: 'failed: ' + e.message });
        }
      }
    }
  }
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'OPTIONS') return json(res, 204, {});
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  try {
    if (req.method === 'GET' && url.pathname === '/')
      return json(res, 200, { ok: true, service: 'ig-auto-dm-backend', version: VERSION });

    if (req.method === 'GET' && url.pathname === '/api/health') {
      const h = await db.health();
      return json(res, 200, { ok: true, version: VERSION, databaseConfigured: true,
        graphConfigured: meta.configured(), historyCount: h.historyCount,
        queueRunning, queuePaused,
        licenseConfigured: db.licenseConfigured() && h.activeLicenseCount > 0,
        activeLicenseCount: h.activeLicenseCount });
    }

    if (req.method === 'GET' && url.pathname === '/api/history')
      return json(res, 200, { items: await db.history() });

    if (req.method === 'POST' && url.pathname === '/api/license/activate') {
      const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
      if (!activationAllowed(ip)) return json(res, 429, { error: 'Too many activation attempts. Please try again later.' });
      const body = parse(await rawBody(req));
      const deviceId = String(body.deviceId || '').trim(), key = String(body.licenseKey || '').trim();
      if (!deviceId || !key) return json(res, 400, { error: 'deviceId and licenseKey are required.' });
      const result = await db.activate(deviceId, key);
      if (result.error) return json(res, result.status, { error: result.error });
      return json(res, 200, { ok: true, plan: result.plan, licenseToken: result.token });
    }

    if (req.method === 'POST' && url.pathname === '/api/license/status')
      return json(res, 200, await db.status(parse(await rawBody(req))));

    if (req.method === 'POST' && url.pathname === '/api/audience') {
      const source = String(parse(await rawBody(req)).source || '');
      if (!['followers', 'following'].includes(source))
        return json(res, 400, { error: 'source must be followers or following.' });
      return json(res, 200, { items: await db.audience(source) });
    }

    if (req.method === 'GET' && url.pathname === '/api/automation/status')
      return json(res, 200, { queueRunning, queuePaused, currentCampaign });

    if (req.method === 'POST' && url.pathname === '/api/automation/pause') {
      const body = parse(await rawBody(req));
      if (!queueRunning || !currentCampaign) return json(res, 409, { error: 'No bulk automation is currently running.' });
      if (String(body.deviceId || '') !== String(currentCampaign.ownerDeviceId || ''))
        return json(res, 403, { error: 'Only the installation that started this automation can pause it.' });
      queuePaused = true; await persistCampaign('paused');
      return json(res, 200, { ok: true, paused: true,
        message: 'Automation paused. The current in-flight request may complete, but no next message will start until resumed.' });
    }

    if (req.method === 'POST' && url.pathname === '/api/automation/resume') {
      const body = parse(await rawBody(req));
      if (!queueRunning || !currentCampaign) return json(res, 409, { error: 'No bulk automation is currently running.' });
      if (String(body.deviceId || '') !== String(currentCampaign.ownerDeviceId || ''))
        return json(res, 403, { error: 'Only the installation that started this automation can resume it.' });
      queuePaused = false; await persistCampaign('running');
      return json(res, 200, { ok: true, paused: false, message: 'Automation resumed.' });
    }

    if (req.method === 'POST' && url.pathname === '/api/monitor/config') {
      await db.saveMonitor(parse(await rawBody(req)));
      return json(res, 200, { ok: true, message: 'Monitor configuration saved.' });
    }

    if (req.method === 'POST' && url.pathname === '/api/bulk') {
      const body = parse(await rawBody(req));
      if (queueRunning) return json(res, 409, { error: 'A campaign is already running.' });
      if (!meta.configured()) return json(res, 503, { error: 'Meta credentials are incomplete on the backend.' });
      const deviceId = String(body.deviceId || '').trim();
      if (!deviceId) return json(res, 400, { error: 'deviceId is required.' });
      const plan = await db.planFor(deviceId, String(body.licenseToken || ''));
      const rows = (Array.isArray(body.rows) ? body.rows : []).filter(eligible);
      if (!rows.length) return json(res, 400, { error: 'No opt-in, messaging-eligible rows with recipient_id found.' });
      const messageTemplate = String(body.template || '').trim();
      if (!messageTemplate) return json(res, 400, { error: 'Message template is required.' });
      const requested = Math.max(1, Math.min(10_000, Number(body.maxRecipients || 20)));
      const used = await db.usageFor(deviceId);
      const remaining = plan === 'pro' ? Number.MAX_SAFE_INTEGER : Math.max(0, 20 - used);
      if (remaining <= 0)
        return json(res, 429, { error: 'Free daily bulk-DM quota reached (20/20). Activate Pro or try again tomorrow.' });
      const accepted = Math.min(rows.length, requested, remaining);
      runBulk(body, plan, rows.slice(0, accepted)).catch(e => console.error('Bulk queue error:', e));
      return json(res, 202, { ok: true, plan, accepted,
        message: plan === 'pro'
          ? 'No IG Auto DM app-level daily quota; Meta/platform limits still apply.'
          : `${remaining - accepted} free bulk DM slot(s) remain after this accepted batch.` });
    }

    if (req.method === 'GET' && url.pathname === '/webhook') {
      const mode = url.searchParams.get('hub.mode'), token = url.searchParams.get('hub.verify_token');
      const challenge = url.searchParams.get('hub.challenge');
      if (mode === 'subscribe' && WEBHOOK_VERIFY_TOKEN && token === WEBHOOK_VERIFY_TOKEN) {
        res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end(challenge || '');
      }
      return json(res, 403, { error: 'Webhook verification failed' });
    }

    if (req.method === 'POST' && url.pathname === '/webhook') {
      const raw = await rawBody(req);
      if (!meta.validSignature(raw, req.headers['x-hub-signature-256']))
        return json(res, 401, { error: 'Invalid webhook signature' });
      await handleWebhook(parse(raw));
      return json(res, 200, { ok: true });
    }

    return json(res, 404, { error: 'Not found' });
  } catch (e) {
    console.error(e); return json(res, 500, { error: e.message || 'Server error' });
  }
});

async function start() {
  await db.init();
  server.listen(PORT, HOST, () => console.log(`IG Auto DM backend v${VERSION} listening at http://${HOST}:${PORT}`));
}
async function shutdown(signal) {
  console.log(`${signal} received; shutting down.`);
  server.close(async () => { await db.close().catch(() => {}); process.exit(0); });
  setTimeout(() => process.exit(1), 10_000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
start().catch(e => { console.error('Startup failed:', e); process.exit(1); });
