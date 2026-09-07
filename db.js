const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Pool } = require('pg');

const DATABASE_URL = process.env.DATABASE_URL || '';
const PGSSL = process.env.PGSSL !== 'false';
const LICENSE_PEPPER = process.env.LICENSE_PEPPER || '';
const LICENSE_FILE = process.env.LICENSE_FILE || path.join(__dirname, 'licenses.json');

if (!DATABASE_URL) throw new Error('DATABASE_URL is required.');

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: PGSSL ? { rejectUnauthorized: false } : false,
  max: 5,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 15_000
});

function dayKey() { return new Date().toISOString().slice(0, 10); }
function tokenHash(token) { return crypto.createHash('sha256').update(String(token)).digest('hex'); }
function secretsToken() { return crypto.randomBytes(32).toString('base64url'); }
function licenseDigest(key) {
  if (!LICENSE_PEPPER) throw new Error('Server LICENSE_PEPPER is not configured.');
  return crypto.createHmac('sha256', Buffer.from(LICENSE_PEPPER, 'hex'))
    .update(String(key).trim().toUpperCase()).digest('hex');
}

async function init() {
  const statements = [
    `CREATE TABLE IF NOT EXISTS licenses(
      id INTEGER PRIMARY KEY,digest CHAR(64) NOT NULL UNIQUE,plan TEXT NOT NULL DEFAULT 'pro',
      status TEXT NOT NULL DEFAULT 'active',max_devices INTEGER NOT NULL DEFAULT 1,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
    `CREATE TABLE IF NOT EXISTS license_activations(
      license_id INTEGER PRIMARY KEY REFERENCES licenses(id) ON DELETE CASCADE,
      device_id TEXT NOT NULL,activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
    `CREATE INDEX IF NOT EXISTS idx_license_activations_device ON license_activations(device_id)`,
    `CREATE TABLE IF NOT EXISTS license_sessions(
      token_hash CHAR(64) PRIMARY KEY,license_id INTEGER NOT NULL REFERENCES licenses(id) ON DELETE CASCADE,
      device_id TEXT NOT NULL,plan TEXT NOT NULL DEFAULT 'pro',created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),expires_at TIMESTAMPTZ,revoked_at TIMESTAMPTZ)`,
    `CREATE INDEX IF NOT EXISTS idx_license_sessions_device ON license_sessions(device_id)`,
    `CREATE TABLE IF NOT EXISTS daily_usage(
      usage_date DATE NOT NULL,device_id TEXT NOT NULL,bulk_messages_sent INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY(usage_date,device_id))`,
    `CREATE TABLE IF NOT EXISTS history(
      id TEXT PRIMARY KEY,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),type TEXT NOT NULL DEFAULT '',
      recipient_id TEXT NOT NULL DEFAULT '',username TEXT NOT NULL DEFAULT '',message TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT '',message_id TEXT NOT NULL DEFAULT '',plan TEXT NOT NULL DEFAULT '')`,
    `CREATE INDEX IF NOT EXISTS idx_history_created_at ON history(created_at DESC)`,
    `CREATE TABLE IF NOT EXISTS monitor_config(
      id SMALLINT PRIMARY KEY CHECK(id=1),enabled BOOLEAN NOT NULL DEFAULT FALSE,
      inbound BOOLEAN NOT NULL DEFAULT TRUE,comments BOOLEAN NOT NULL DEFAULT FALSE,message TEXT NOT NULL DEFAULT '')`,
    `INSERT INTO monitor_config(id) VALUES(1) ON CONFLICT(id) DO NOTHING`,
    `CREATE TABLE IF NOT EXISTS audience(
      recipient_id TEXT PRIMARY KEY,username TEXT NOT NULL DEFAULT '',first_name TEXT NOT NULL DEFAULT '',
      opt_in BOOLEAN NOT NULL DEFAULT FALSE,messaging_eligible BOOLEAN NOT NULL DEFAULT FALSE,
      relation TEXT NOT NULL DEFAULT '',data JSONB NOT NULL DEFAULT '{}'::jsonb,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
    `CREATE INDEX IF NOT EXISTS idx_audience_relation ON audience(relation,opt_in,messaging_eligible)`,
    `CREATE TABLE IF NOT EXISTS campaigns(
      id TEXT PRIMARY KEY,name TEXT NOT NULL,owner_device_id TEXT NOT NULL,plan TEXT NOT NULL,
      accepted INTEGER NOT NULL DEFAULT 0,processed INTEGER NOT NULL DEFAULT 0,sent INTEGER NOT NULL DEFAULT 0,
      failed INTEGER NOT NULL DEFAULT 0,skipped INTEGER NOT NULL DEFAULT 0,state TEXT NOT NULL DEFAULT 'running',
      started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),finished_at TIMESTAMPTZ,
      current_recipient_id TEXT NOT NULL DEFAULT '',current_username TEXT NOT NULL DEFAULT '')`
  ];
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const sql of statements) await client.query(sql);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally { client.release(); }
  await seedLicenses();
}

async function seedLicenses() {
  let data = { licenses: [] };
  try { data = JSON.parse(fs.readFileSync(LICENSE_FILE, 'utf8')); }
  catch (e) { console.error('License seed file error:', e.message); return; }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const lic of data.licenses || []) {
      await client.query(
        `INSERT INTO licenses(id,digest,plan,status,max_devices) VALUES($1,$2,$3,$4,$5)
         ON CONFLICT(id) DO UPDATE SET digest=EXCLUDED.digest,plan=EXCLUDED.plan,
         status=EXCLUDED.status,max_devices=EXCLUDED.max_devices`,
        [lic.id, lic.digest, lic.plan || 'pro', lic.status || 'active', Number(lic.maxDevices || 1)]
      );
    }
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; }
  finally { client.release(); }
}

async function health() {
  const [{ rows: l }, { rows: h }] = await Promise.all([
    pool.query("SELECT COUNT(*)::int AS count FROM licenses WHERE status='active'"),
    pool.query('SELECT COUNT(*)::int AS count FROM history')
  ]);
  return { activeLicenseCount: Number(l[0]?.count || 0), historyCount: Number(h[0]?.count || 0) };
}

async function addHistory(item) {
  await pool.query(
    `INSERT INTO history(id,type,recipient_id,username,message,status,message_id,plan)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
    [crypto.randomUUID(), String(item.type || ''), String(item.recipientId || ''), String(item.username || ''),
      String(item.message || ''), String(item.status || ''), String(item.messageId || ''), String(item.plan || '')]
  );
}

async function history() {
  const { rows } = await pool.query(
    `SELECT id,created_at AS at,type,recipient_id AS "recipientId",username,message,status,
            message_id AS "messageId",plan FROM history ORDER BY created_at DESC LIMIT 3000`
  );
  return rows;
}

async function planFor(deviceId, token) {
  if (!deviceId || !token) return 'free';
  const h = tokenHash(token);
  const { rows } = await pool.query(
    `SELECT s.plan FROM license_sessions s JOIN licenses l ON l.id=s.license_id
      JOIN license_activations a ON a.license_id=s.license_id AND a.device_id=s.device_id
      WHERE s.token_hash=$1 AND s.device_id=$2 AND s.revoked_at IS NULL
        AND (s.expires_at IS NULL OR s.expires_at>NOW()) AND l.status='active' LIMIT 1`,
    [h, deviceId]
  );
  if (!rows.length || rows[0].plan !== 'pro') return 'free';
  await Promise.all([
    pool.query('UPDATE license_sessions SET last_seen_at=NOW() WHERE token_hash=$1', [h]),
    pool.query(`UPDATE license_activations a SET last_seen_at=NOW() FROM license_sessions s
                WHERE s.token_hash=$1 AND a.license_id=s.license_id`, [h])
  ]);
  return 'pro';
}

async function usageFor(deviceId) {
  const { rows } = await pool.query(
    `SELECT bulk_messages_sent FROM daily_usage WHERE usage_date=$1::date AND device_id=$2`,
    [dayKey(), deviceId]
  );
  return Number(rows[0]?.bulk_messages_sent || 0);
}

async function addUsage(deviceId, count = 1) {
  const { rows } = await pool.query(
    `INSERT INTO daily_usage(usage_date,device_id,bulk_messages_sent) VALUES($1::date,$2,$3)
     ON CONFLICT(usage_date,device_id) DO UPDATE SET bulk_messages_sent=daily_usage.bulk_messages_sent+EXCLUDED.bulk_messages_sent
     RETURNING bulk_messages_sent`, [dayKey(), deviceId, count]
  );
  return Number(rows[0]?.bulk_messages_sent || 0);
}

async function status(body) {
  const deviceId = String(body.deviceId || '');
  const plan = await planFor(deviceId, String(body.licenseToken || ''));
  const usedToday = await usageFor(deviceId);
  return { plan, usedToday, dailyLimit: plan === 'pro' ? null : 20,
    remaining: plan === 'pro' ? null : Math.max(0, 20 - usedToday) };
}

async function activate(deviceId, key) {
  const digest = licenseDigest(key);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const lr = await client.query(
      `SELECT id,plan FROM licenses WHERE digest=$1 AND status='active' LIMIT 1 FOR UPDATE`, [digest]);
    const lic = lr.rows[0];
    if (!lic) { await client.query('ROLLBACK'); return { status: 403, error: 'Invalid or inactive premium license key.' }; }
    const br = await client.query('SELECT device_id FROM license_activations WHERE license_id=$1 FOR UPDATE', [lic.id]);
    const binding = br.rows[0];
    if (binding && binding.device_id !== deviceId) {
      await client.query('ROLLBACK');
      return { status: 409, error: 'This license is already activated on another installation.' };
    }
    await client.query(
      `INSERT INTO license_activations(license_id,device_id,activated_at,last_seen_at) VALUES($1,$2,NOW(),NOW())
       ON CONFLICT(license_id) DO UPDATE SET last_seen_at=NOW()`, [lic.id, deviceId]);
    await client.query(
      `UPDATE license_sessions SET revoked_at=NOW() WHERE revoked_at IS NULL AND (device_id=$1 OR license_id=$2)`,
      [deviceId, lic.id]);
    const token = secretsToken();
    await client.query(
      `INSERT INTO license_sessions(token_hash,license_id,device_id,plan) VALUES($1,$2,$3,$4)`,
      [tokenHash(token), lic.id, deviceId, lic.plan || 'pro']);
    await client.query('COMMIT');
    return { status: 200, token, plan: 'pro' };
  } catch (e) { await client.query('ROLLBACK').catch(() => {}); throw e; }
  finally { client.release(); }
}

async function getMonitor() {
  const { rows } = await pool.query('SELECT enabled,inbound,comments,message FROM monitor_config WHERE id=1');
  return rows[0] || { enabled: false, inbound: true, comments: false, message: '' };
}

async function saveMonitor(body) {
  await pool.query(
    `INSERT INTO monitor_config(id,enabled,inbound,comments,message) VALUES(1,$1,$2,$3,$4)
     ON CONFLICT(id) DO UPDATE SET enabled=EXCLUDED.enabled,inbound=EXCLUDED.inbound,
       comments=EXCLUDED.comments,message=EXCLUDED.message`,
    [Boolean(body.enabled), Boolean(body.inbound), Boolean(body.comments), String(body.message || '')]
  );
}

async function audience(source) {
  const { rows } = await pool.query(
    `SELECT recipient_id,username,first_name,opt_in,messaging_eligible,relation,data FROM audience
      WHERE relation=$1 AND opt_in=TRUE AND messaging_eligible=TRUE ORDER BY updated_at DESC`, [source]);
  return rows.map(r => ({ ...(r.data || {}), recipient_id: r.recipient_id, username: r.username,
    first_name: r.first_name, opt_in: 'yes', messaging_eligible: 'yes', relation: r.relation }));
}

async function saveCampaign(c, state) {
  if (!c) return;
  await pool.query(
    `INSERT INTO campaigns(id,name,owner_device_id,plan,accepted,processed,sent,failed,skipped,state,
      started_at,finished_at,current_recipient_id,current_username)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
     ON CONFLICT(id) DO UPDATE SET processed=EXCLUDED.processed,sent=EXCLUDED.sent,failed=EXCLUDED.failed,
      skipped=EXCLUDED.skipped,state=EXCLUDED.state,finished_at=EXCLUDED.finished_at,
      current_recipient_id=EXCLUDED.current_recipient_id,current_username=EXCLUDED.current_username`,
    [c.id, c.name, c.ownerDeviceId, c.plan, c.accepted, c.processed, c.sent, c.failed, c.skipped,
      state, c.startedAt, c.finishedAt || null, c.currentRecipientId || '', c.currentUsername || '']
  );
}

async function close() { await pool.end(); }

module.exports = { init, health, addHistory, history, planFor, usageFor, addUsage, status, activate,
  getMonitor, saveMonitor, audience, saveCampaign, close, licenseConfigured: () => Boolean(LICENSE_PEPPER) };
