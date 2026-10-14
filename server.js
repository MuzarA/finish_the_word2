'use strict';
/*
  Finish the Word: relay server.
  Keeps the game rooms in memory. Players talk to it over plain HTTPS (POST) and receive live updates
  through a Server-Sent Events stream, so it works in any network. No dependencies: run with `node server.js`.
*/
const http = require('http');

const PORT = Number(process.env.PORT) || 3000;
const GRACE_MS = Number(process.env.GRACE_MS) || 10000;      // how long a dropped player may come back
const MAX_ROOMS = 300;
const MAX_BODY = 128 * 1024;
const ROOM_TTL = 3 * 3600 * 1000;                            // rooms older than this are removed
const EMPTY_TTL = 10 * 60 * 1000;                            // rooms nobody is connected to
const CODE_RE = /^[A-Z0-9]{4}$/;

// code -> { doc, rev, streams:Set<{res,uid}>, timers:Map<uid,Timeout>, created, touched }
const rooms = new Map();

/* ---------- game rules needed on the server (same as in the page) ---------- */
function isAlive(room, id) { const p = room.players && room.players[id]; return !!p && !p.left && p.lives > 0; }
function aliveIds(room) { return (room.order || []).filter((id) => isAlive(room, id)); }
function nextAlive(room, fromId) {
  const ord = room.order || [], n = ord.length, i = ord.indexOf(fromId);
  for (let k = 1; k <= n; k++) { const id = ord[(i + k + n) % n]; if (isAlive(room, id)) return id; }
  return null;
}
function evId() { return Math.random().toString(36).slice(2, 9); }
function planLeaveGame(room, uid) {
  const merged = Object.assign({}, room.players);
  merged[uid] = Object.assign({}, room.players[uid], { left: true, lives: 0 });
  const tmp = Object.assign({}, room, { players: merged });
  const alive = aliveIds(tmp);
  const pl = {}; pl[uid] = { left: true, lives: 0 };
  const patch = { players: pl, ev: { id: evId(), t: 'left', by: uid } };
  if (alive.length <= 1) { patch.status = 'finished'; patch.winner = alive[0] || null; patch.cur = null; patch.seq = room.seq + 1; }
  else if (room.cur === uid) { patch.seq = room.seq + 1; patch.cur = nextAlive(tmp, uid); }
  return patch;
}

/* ---------- helpers ---------- */
function clone(x) { return x === undefined ? undefined : JSON.parse(JSON.stringify(x)); }
const BAD_KEYS = new Set(['__proto__', 'constructor', 'prototype']);
function merge(dst, src) {
  for (const k of Object.keys(src)) {
    if (BAD_KEYS.has(k)) continue;
    const v = src[k];
    if (v && typeof v === 'object' && !Array.isArray(v) && dst[k] && typeof dst[k] === 'object' && !Array.isArray(dst[k])) merge(dst[k], v);
    else dst[k] = clone(v);
  }
}
function isObj(x) { return !!x && typeof x === 'object' && !Array.isArray(x); }
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Access-Control-Max-Age', '86400');
}
function send(res, status, obj) {
  setCors(res);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > MAX_BODY) { reject(Object.assign(new Error('too large'), { status: 413 })); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}); }
      catch (e) { reject(Object.assign(new Error('bad json'), { status: 400 })); }
    });
    req.on('error', reject);
  });
}
function broadcast(room, msg) {
  const line = 'data: ' + JSON.stringify(msg) + '\n\n';
  for (const s of room.streams) { try { s.res.write(line); } catch (e) { /* stream is gone */ } }
}
function applyPatch(code, patch) {
  const room = rooms.get(code);
  if (!room) return null;
  merge(room.doc, patch);
  room.rev += 1; room.touched = Date.now();
  broadcast(room, { t: 'doc', doc: room.doc, rev: room.rev });
  return room;
}
function closeRoom(code) {
  const room = rooms.get(code);
  if (!room) return;
  rooms.delete(code);
  broadcast(room, { t: 'closed' });
  for (const s of room.streams) { try { s.res.end(); } catch (e) { /* ignore */ } }
  for (const t of room.timers.values()) clearTimeout(t);
  room.streams.clear(); room.timers.clear();
}
function activeCount(doc) { return Object.keys(doc.players || {}).filter((id) => !doc.players[id].left).length; }
function playerGone(code, uid) {
  const room = rooms.get(code);
  if (!room) return;
  const doc = room.doc, p = doc.players && doc.players[uid];
  if (!p || p.left) return;
  const patch = doc.status === 'playing' ? planLeaveGame(doc, uid) : { players: { [uid]: { left: true } } };
  applyPatch(code, patch);
}

/* ---------- routes ---------- */
function openStream(req, res, url) {
  const code = String(url.searchParams.get('code') || '').toUpperCase();
  const uid = String(url.searchParams.get('uid') || '').slice(0, 64);
  setCors(res);
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
  const room = rooms.get(code);
  if (!room) { res.write('data: ' + JSON.stringify({ t: 'closed' }) + '\n\n'); res.end(); return; }
  res.write('retry: 2000\n\n');
  const s = { res, uid };
  room.streams.add(s);
  const pending = room.timers.get(uid);
  if (pending) { clearTimeout(pending); room.timers.delete(uid); }
  res.write('data: ' + JSON.stringify({ t: 'doc', doc: room.doc, rev: room.rev }) + '\n\n');
  req.on('close', () => {
    room.streams.delete(s);
    if (!uid) return;
    for (const o of room.streams) if (o.uid === uid) return;       // the same player still has another stream open
    const t = setTimeout(() => { room.timers.delete(uid); if (rooms.get(code) === room) playerGone(code, uid); }, GRACE_MS);
    room.timers.set(uid, t);
  });
}

function purge() {
  const now = Date.now();
  for (const [code, room] of [...rooms]) {
    if (now - room.touched > ROOM_TTL || (room.streams.size === 0 && now - room.touched > EMPTY_TTL)) closeRoom(code);
  }
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    if (req.method === 'OPTIONS') { setCors(res); res.writeHead(204); res.end(); return; }
    if (req.method === 'GET' && url.pathname === '/') {
      setCors(res); res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Finish the Word server is running. Rooms: ' + rooms.size + '\n'); return;
    }
    if (req.method === 'GET' && url.pathname === '/events') { openStream(req, res, url); return; }
    if (req.method === 'GET' && url.pathname === '/room') {
      const room = rooms.get(String(url.searchParams.get('code') || '').toUpperCase());
      if (!room) { send(res, 404, { error: 'not found' }); return; }
      send(res, 200, { doc: room.doc, rev: room.rev }); return;
    }
    if (req.method === 'GET' && url.pathname === '/rooms') {
      const now = Date.now(), list = [];
      for (const room of rooms.values()) {
        const n = activeCount(room.doc);
        if (room.doc.status === 'lobby' && now - room.created < ROOM_TTL && n >= 1 && n < 8) list.push(room.doc);
      }
      send(res, 200, { rooms: list }); return;
    }
    if (req.method === 'POST') {
      const body = await readBody(req);
      const code = String(body.code || '').toUpperCase();
      if (!CODE_RE.test(code)) { send(res, 400, { error: 'bad code' }); return; }
      if (url.pathname === '/create') {
        if (rooms.has(code)) { send(res, 409, { error: 'exists' }); return; }
        if (!isObj(body.doc) || !isObj(body.doc.players)) { send(res, 400, { error: 'bad room' }); return; }
        if (rooms.size >= MAX_ROOMS) purge();
        if (rooms.size >= MAX_ROOMS) { send(res, 503, { error: 'full' }); return; }
        const doc = clone(body.doc); doc.code = code;
        rooms.set(code, { doc, rev: 1, streams: new Set(), timers: new Map(), created: Date.now(), touched: Date.now() });
        send(res, 200, { ok: true, rev: 1 }); return;
      }
      if (url.pathname === '/patch') {
        if (!isObj(body.patch)) { send(res, 400, { error: 'bad patch' }); return; }
        const room = applyPatch(code, body.patch);
        if (!room) { send(res, 404, { error: 'not found' }); return; }
        send(res, 200, { ok: true, doc: room.doc, rev: room.rev }); return;
      }
      if (url.pathname === '/delete') { closeRoom(code); send(res, 200, { ok: true }); return; }
    }
    send(res, 404, { error: 'not found' });
  } catch (e) {
    try { send(res, e.status || 500, { error: String((e && e.message) || e) }); } catch (x) { /* response already started */ }
  }
});

setInterval(() => {
  for (const room of rooms.values()) for (const s of room.streams) { try { s.res.write(': ping\n\n'); } catch (e) { /* ignore */ } }
}, 20000);
setInterval(purge, 60000);

server.listen(PORT, '0.0.0.0', () => console.log('Finish the Word server listening on ' + PORT));
