const crypto = require('crypto');
const GRAPH_BASE_URL = (process.env.GRAPH_BASE_URL || 'https://graph.instagram.com').replace(/\/$/, '');
const GRAPH_VERSION = process.env.GRAPH_VERSION || 'v23.0';
const IG_USER_ID = process.env.IG_USER_ID || '';
const IG_ACCESS_TOKEN = process.env.IG_ACCESS_TOKEN || '';
const META_APP_SECRET = process.env.META_APP_SECRET || '';

function configured() { return Boolean(GRAPH_VERSION && IG_USER_ID && IG_ACCESS_TOKEN); }
function url() {
  if (!configured()) throw new Error('Meta credentials are incomplete.');
  return `${GRAPH_BASE_URL}/${GRAPH_VERSION}/${IG_USER_ID}/messages`;
}
async function post(body) {
  const resp = await fetch(url(), { method: 'POST', headers: {
    Authorization: `Bearer ${IG_ACCESS_TOKEN}`, 'Content-Type': 'application/json'
  }, body: JSON.stringify(body) });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(data?.error?.message || `Meta API HTTP ${resp.status}`);
  return data;
}
function sendText(recipientId, message) {
  return post({ recipient: { id: String(recipientId) }, message: { text: String(message) } });
}
function sendPrivateReply(commentId, message) {
  return post({ recipient: { comment_id: String(commentId) }, message: { text: String(message) } });
}
function validSignature(raw, signature) {
  if (!META_APP_SECRET) return true;
  if (!signature || !signature.startsWith('sha256=')) return false;
  const expected = 'sha256=' + crypto.createHmac('sha256', META_APP_SECRET).update(raw).digest('hex');
  const a = Buffer.from(expected), b = Buffer.from(signature);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
module.exports = { configured, sendText, sendPrivateReply, validSignature };
