#!/usr/bin/env npx tsx
/**
 * Connection Timeout Test
 *
 * Standalone server to identify timeout limits on any hosting platform.
 * Tests SSE, WebSocket, and long-running HTTP responses.
 *
 * Manual: open http://<host>:3333 for the interactive dashboard.
 * Automated: open http://<host>:3333/suite for the automated test suite.
 */

import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { createHash } from "node:crypto";
import { URL } from "node:url";

const PORT = (() => {
  const idx = process.argv.indexOf("--port");
  return idx !== -1 && process.argv[idx + 1]
    ? Number(process.argv[idx + 1])
    : 3333;
})();

// ── Suite HTML ──────────────────────────────────────────────────────
const SUITE_HTML = `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8">
<title>Connection Timeout Test Suite</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: monospace; background: #1a1a1a; color: #e0e0e0; padding: 20px; max-width: 900px; }
  h1 { margin-bottom: 8px; }
  .subtitle { color: #888; margin-bottom: 20px; font-size: 13px; }
  button.run { background: #363; border: 1px solid #585; color: #afa; padding: 8px 20px;
    border-radius: 4px; cursor: pointer; font-family: monospace; font-size: 14px; margin-bottom: 20px; }
  button.run:hover { background: #474; }
  button.run:disabled { opacity: 0.4; cursor: default; }
  table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
  th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #333; font-size: 13px; }
  th { color: #88f; border-bottom: 2px solid #444; }
  .pass { color: #8f8; }
  .fail { color: #f88; }
  .running { color: #ff8; }
  .skip { color: #888; }
  .log { font-size: 12px; background: #111; padding: 12px; border-radius: 4px;
    white-space: pre-wrap; max-height: 400px; overflow-y: auto; }
  .summary { background: #2a2a2a; border: 1px solid #444; border-radius: 8px; padding: 16px;
    margin-bottom: 20px; }
  .summary h2 { font-size: 14px; color: #88f; margin-bottom: 8px; }
</style>
</head>
<body>
<h1>Connection Timeout Test Suite</h1>
<p class="subtitle">Automatisierte Tests um Timeout-Limits der Hosting-Plattform zu identifizieren.</p>
<button class="run" id="run-btn" onclick="runSuite()">Suite starten</button>

<div class="summary" id="summary" style="display:none">
  <h2>Ergebnis</h2>
  <div id="summary-text"></div>
</div>

<table>
  <thead>
    <tr><th>#</th><th>Test</th><th>Parameter</th><th>Erwartet</th><th>Ergebnis</th><th>Details</th></tr>
  </thead>
  <tbody id="results"></tbody>
</table>

<h2 style="font-size:14px; color:#88f; margin-bottom:8px;">Log</h2>
<div class="log" id="log"></div>

<script>
const BASE = location.origin;
let running = false;

function log(msg) {
  const el = document.getElementById('log');
  el.textContent += new Date().toLocaleTimeString() + ' ' + msg + '\\n';
  el.scrollTop = el.scrollHeight;
}

function addRow(id, name, params, expected) {
  const tbody = document.getElementById('results');
  const tr = document.createElement('tr');
  tr.id = 'row-' + id;
  tr.innerHTML = '<td>' + id + '</td><td>' + name + '</td><td>' + params + '</td>'
    + '<td>' + expected + '</td><td class="running" id="result-' + id + '">...</td>'
    + '<td id="detail-' + id + '"></td>';
  tbody.appendChild(tr);
}

function setResult(id, passed, text, detail) {
  const el = document.getElementById('result-' + id);
  el.textContent = text;
  el.className = passed ? 'pass' : 'fail';
  document.getElementById('detail-' + id).textContent = detail || '';
}

// ── Test helpers ────────────────────────────────────────────────────

/** SSE test: connect, wait for disconnect or maxWait, return elapsed seconds */
function testSSE(pause, heartbeat, maxWaitS) {
  return new Promise((resolve) => {
    const start = Date.now();
    const url = BASE + '/sse?pause=' + pause + '&heartbeat=' + heartbeat;
    const es = new EventSource(url);
    let events = 0;
    let lastEvent = 0;
    const timeout = setTimeout(() => {
      es.close();
      resolve({ elapsed: (Date.now() - start) / 1000, events, survived: true });
    }, maxWaitS * 1000);

    es.addEventListener('ping', () => { events++; lastEvent = Date.now(); });
    es.addEventListener('heartbeat', () => { lastEvent = Date.now(); });
    es.onerror = () => {
      clearTimeout(timeout);
      es.close();
      resolve({ elapsed: (Date.now() - start) / 1000, events, survived: false });
    };
  });
}

/** SSE silent: no events at all, just hold connection */
function testSSESilent(maxWaitS) {
  return new Promise((resolve) => {
    const start = Date.now();
    const es = new EventSource(BASE + '/sse-silent');
    const timeout = setTimeout(() => {
      es.close();
      resolve({ elapsed: (Date.now() - start) / 1000, survived: true });
    }, maxWaitS * 1000);

    es.onerror = () => {
      clearTimeout(timeout);
      es.close();
      resolve({ elapsed: (Date.now() - start) / 1000, survived: false });
    };
  });
}

/** WebSocket test */
function testWS(pause, maxWaitS) {
  return new Promise((resolve) => {
    const start = Date.now();
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(proto + '://' + location.host + '/ws?pause=' + pause);
    let messages = 0;
    const timeout = setTimeout(() => {
      ws.close();
      resolve({ elapsed: (Date.now() - start) / 1000, messages, survived: true });
    }, maxWaitS * 1000);

    ws.onmessage = () => { messages++; };
    ws.onclose = () => {
      clearTimeout(timeout);
      resolve({ elapsed: (Date.now() - start) / 1000, messages, survived: false });
    };
    ws.onerror = () => {
      clearTimeout(timeout);
      ws.close();
      resolve({ elapsed: (Date.now() - start) / 1000, messages, survived: false });
    };
  });
}

/** HTTP long response test */
function testHTTP(delayS, maxWaitS) {
  return new Promise((resolve) => {
    const start = Date.now();
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort();
      resolve({ elapsed: (Date.now() - start) / 1000, survived: false, timedOut: true });
    }, maxWaitS * 1000);

    fetch(BASE + '/http?delay=' + delayS, { signal: controller.signal })
      .then(r => r.text())
      .then(() => {
        clearTimeout(timeout);
        resolve({ elapsed: (Date.now() - start) / 1000, survived: true });
      })
      .catch(() => {
        clearTimeout(timeout);
        resolve({ elapsed: (Date.now() - start) / 1000, survived: false });
      });
  });
}

/** Chunked transfer test */
function testChunked(pause, maxWaitS) {
  return new Promise((resolve) => {
    const start = Date.now();
    const controller = new AbortController();
    let chunks = 0;
    const timeout = setTimeout(() => {
      controller.abort();
      resolve({ elapsed: (Date.now() - start) / 1000, chunks, survived: true });
    }, maxWaitS * 1000);

    fetch(BASE + '/chunked?pause=' + pause, { signal: controller.signal })
      .then(async r => {
        const reader = r.body.getReader();
        for (;;) {
          const { done } = await reader.read();
          if (done) break;
          chunks++;
        }
        clearTimeout(timeout);
        resolve({ elapsed: (Date.now() - start) / 1000, chunks, survived: true });
      })
      .catch(() => {
        clearTimeout(timeout);
        resolve({ elapsed: (Date.now() - start) / 1000, chunks, survived: false });
      });
  });
}

// ── Suite ───────────────────────────────────────────────────────────

async function runSuite() {
  if (running) return;
  running = true;
  document.getElementById('run-btn').disabled = true;
  document.getElementById('results').innerHTML = '';
  document.getElementById('log').textContent = '';
  document.getElementById('summary').style.display = 'none';

  const results = [];

  // Test 1: SSE baseline (5s pause, should work)
  addRow(1, 'SSE Baseline', 'pause=5s', 'Verbunden bleiben');
  log('Test 1: SSE Baseline (5s pause, 20s max)');
  const t1 = await testSSE(5, 0, 20);
  const t1pass = t1.survived && t1.events >= 2;
  setResult(1, t1pass, t1pass ? 'OK' : 'FAIL', t1.events + ' events, ' + t1.elapsed.toFixed(1) + 's');
  results.push({ name: 'SSE Baseline', pass: t1pass, detail: t1 });
  log('  -> ' + (t1pass ? 'PASS' : 'FAIL') + ': ' + t1.events + ' events in ' + t1.elapsed.toFixed(1) + 's');

  // Test 2: SSE 30s pause (common proxy timeout boundary)
  addRow(2, 'SSE 30s Pause', 'pause=30s', 'Verbunden bleiben');
  log('Test 2: SSE 30s Pause (70s max)');
  const t2 = await testSSE(30, 0, 70);
  const t2pass = t2.survived || t2.events >= 2;
  setResult(2, t2pass, t2pass ? 'OK' : 'FAIL @ ' + t2.elapsed.toFixed(0) + 's',
    t2.events + ' events, ' + t2.elapsed.toFixed(1) + 's');
  results.push({ name: 'SSE 30s', pass: t2pass, detail: t2 });
  log('  -> ' + (t2pass ? 'PASS' : 'FAIL') + ': ' + t2.elapsed.toFixed(1) + 's, ' + t2.events + ' events');

  // Test 3: SSE 60s pause
  addRow(3, 'SSE 60s Pause', 'pause=60s', 'Verbunden bleiben');
  log('Test 3: SSE 60s Pause (130s max)');
  const t3 = await testSSE(60, 0, 130);
  const t3pass = t3.survived || t3.events >= 2;
  setResult(3, t3pass, t3pass ? 'OK' : 'FAIL @ ' + t3.elapsed.toFixed(0) + 's',
    t3.events + ' events, ' + t3.elapsed.toFixed(1) + 's');
  results.push({ name: 'SSE 60s', pass: t3pass, detail: t3 });
  log('  -> ' + (t3pass ? 'PASS' : 'FAIL') + ': ' + t3.elapsed.toFixed(1) + 's, ' + t3.events + ' events');

  // Test 4: SSE 120s pause
  addRow(4, 'SSE 120s Pause', 'pause=120s', 'Timeout finden');
  log('Test 4: SSE 120s Pause (250s max)');
  const t4 = await testSSE(120, 0, 250);
  const t4pass = t4.survived;
  setResult(4, t4pass, t4pass ? 'OK' : 'FAIL @ ' + t4.elapsed.toFixed(0) + 's',
    t4.events + ' events, ' + t4.elapsed.toFixed(1) + 's');
  results.push({ name: 'SSE 120s', pass: t4pass, detail: t4 });
  log('  -> ' + (t4pass ? 'PASS' : 'FAIL') + ': ' + t4.elapsed.toFixed(1) + 's');

  // Test 5: SSE 120s pause + 15s heartbeat (does heartbeat help?)
  addRow(5, 'SSE 120s + Heartbeat', 'pause=120s, hb=15s', 'Heartbeat rettet Connection');
  log('Test 5: SSE 120s Pause + 15s Heartbeat (250s max)');
  const t5 = await testSSE(120, 15, 250);
  const t5pass = t5.survived;
  setResult(5, t5pass, t5pass ? 'OK' : 'FAIL @ ' + t5.elapsed.toFixed(0) + 's',
    t5.events + ' events, ' + t5.elapsed.toFixed(1) + 's');
  results.push({ name: 'SSE 120s+HB', pass: t5pass, detail: t5 });
  log('  -> ' + (t5pass ? 'PASS' : 'FAIL') + ': ' + t5.elapsed.toFixed(1) + 's');

  // Test 6: SSE Silent (pure idle timeout)
  addRow(6, 'SSE Silent', 'keine Events', 'Idle-Timeout finden');
  log('Test 6: SSE Silent (180s max)');
  const t6 = await testSSESilent(180);
  setResult(6, false, t6.survived ? 'kein Timeout in 180s' : 'Timeout @ ' + t6.elapsed.toFixed(0) + 's',
    t6.elapsed.toFixed(1) + 's');
  results.push({ name: 'SSE Silent', pass: t6.survived, detail: t6 });
  log('  -> Timeout nach ' + t6.elapsed.toFixed(1) + 's');

  // Test 7: WebSocket 30s
  addRow(7, 'WebSocket 30s', 'pause=30s', 'Verbunden bleiben');
  log('Test 7: WebSocket 30s Pause (70s max)');
  const t7 = await testWS(30, 70);
  const t7pass = t7.survived || t7.messages >= 2;
  setResult(7, t7pass, t7pass ? 'OK' : 'FAIL @ ' + t7.elapsed.toFixed(0) + 's',
    t7.messages + ' msgs, ' + t7.elapsed.toFixed(1) + 's');
  results.push({ name: 'WS 30s', pass: t7pass, detail: t7 });
  log('  -> ' + (t7pass ? 'PASS' : 'FAIL') + ': ' + t7.elapsed.toFixed(1) + 's');

  // Test 8: WebSocket 60s
  addRow(8, 'WebSocket 60s', 'pause=60s', 'Timeout finden');
  log('Test 8: WebSocket 60s Pause (130s max)');
  const t8 = await testWS(60, 130);
  const t8pass = t8.survived || t8.messages >= 2;
  setResult(8, t8pass, t8pass ? 'OK' : 'FAIL @ ' + t8.elapsed.toFixed(0) + 's',
    t8.messages + ' msgs, ' + t8.elapsed.toFixed(1) + 's');
  results.push({ name: 'WS 60s', pass: t8pass, detail: t8 });
  log('  -> ' + (t8pass ? 'PASS' : 'FAIL') + ': ' + t8.elapsed.toFixed(1) + 's');

  // Test 9: HTTP 30s delay
  addRow(9, 'HTTP 30s Delay', 'delay=30s', 'Response erhalten');
  log('Test 9: HTTP 30s Delay (45s max)');
  const t9 = await testHTTP(30, 45);
  setResult(9, t9.survived, t9.survived ? 'OK' : 'FAIL @ ' + t9.elapsed.toFixed(0) + 's',
    t9.elapsed.toFixed(1) + 's');
  results.push({ name: 'HTTP 30s', pass: t9.survived, detail: t9 });
  log('  -> ' + (t9.survived ? 'PASS' : 'FAIL') + ': ' + t9.elapsed.toFixed(1) + 's');

  // Test 10: HTTP 90s delay
  addRow(10, 'HTTP 90s Delay', 'delay=90s', 'Timeout finden');
  log('Test 10: HTTP 90s Delay (120s max)');
  const t10 = await testHTTP(90, 120);
  setResult(10, t10.survived, t10.survived ? 'OK' : 'FAIL @ ' + t10.elapsed.toFixed(0) + 's',
    t10.elapsed.toFixed(1) + 's');
  results.push({ name: 'HTTP 90s', pass: t10.survived, detail: t10 });
  log('  -> ' + (t10.survived ? 'PASS' : 'FAIL') + ': ' + t10.elapsed.toFixed(1) + 's');

  // Test 11: Chunked 30s
  addRow(11, 'Chunked 30s', 'pause=30s', 'Verbunden bleiben');
  log('Test 11: Chunked 30s Pause (70s max)');
  const t11 = await testChunked(30, 70);
  const t11pass = t11.survived || t11.chunks >= 2;
  setResult(11, t11pass, t11pass ? 'OK' : 'FAIL @ ' + t11.elapsed.toFixed(0) + 's',
    t11.chunks + ' chunks, ' + t11.elapsed.toFixed(1) + 's');
  results.push({ name: 'Chunked 30s', pass: t11pass, detail: t11 });
  log('  -> ' + (t11pass ? 'PASS' : 'FAIL') + ': ' + t11.elapsed.toFixed(1) + 's');

  // Test 12: Chunked 60s
  addRow(12, 'Chunked 60s', 'pause=60s', 'Timeout finden');
  log('Test 12: Chunked 60s Pause (130s max)');
  const t12 = await testChunked(60, 130);
  const t12pass = t12.survived || t12.chunks >= 2;
  setResult(12, t12pass, t12pass ? 'OK' : 'FAIL @ ' + t12.elapsed.toFixed(0) + 's',
    t12.chunks + ' chunks, ' + t12.elapsed.toFixed(1) + 's');
  results.push({ name: 'Chunked 60s', pass: t12pass, detail: t12 });
  log('  -> ' + (t12pass ? 'PASS' : 'FAIL') + ': ' + t12.elapsed.toFixed(1) + 's');

  // Summary
  const passed = results.filter(r => r.pass).length;
  const failed = results.filter(r => !r.pass).length;
  const sseTimeout = !t3.survived ? t3.elapsed : !t4.survived ? t4.elapsed : null;
  const wsTimeout = !t8.survived ? t8.elapsed : null;
  const silentTimeout = !t6.survived ? t6.elapsed : null;
  const heartbeatHelps = !t4.survived && t5.survived;

  let summary = passed + '/' + results.length + ' Tests bestanden\\n\\n';
  if (silentTimeout) summary += 'Idle-Timeout (keine Daten): ~' + Math.round(silentTimeout) + 's\\n';
  if (sseTimeout) summary += 'SSE-Timeout (mit Pausen): ~' + Math.round(sseTimeout) + 's\\n';
  if (wsTimeout) summary += 'WebSocket-Timeout: ~' + Math.round(wsTimeout) + 's\\n';
  if (heartbeatHelps) summary += '\\nHeartbeat (15s) rettet die Connection!\\n';
  else if (!t4.survived && !t5.survived) summary += '\\nHeartbeat hilft NICHT — Proxy hat harten Timeout.\\n';
  if (!sseTimeout && !wsTimeout && !silentTimeout) summary += '\\nKeine Timeouts erkannt — Plattform scheint keine idle-Limits zu haben.\\n';

  document.getElementById('summary').style.display = '';
  document.getElementById('summary-text').textContent = summary;
  log('\\n=== FERTIG ===\\n' + summary);

  running = false;
  document.getElementById('run-btn').disabled = false;
}
</script>
</body>
</html>`;

// ── Manual Dashboard HTML ───────────────────────────────────────────
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8">
<title>Connection Timeout Test</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: monospace; background: #1a1a1a; color: #e0e0e0; padding: 20px; }
  h1 { margin-bottom: 8px; }
  .nav { margin-bottom: 20px; font-size: 13px; }
  .nav a { color: #88f; }
  .tests { display: grid; gap: 16px; max-width: 800px; }
  .test { background: #2a2a2a; border: 1px solid #444; border-radius: 8px; padding: 16px; }
  .test h2 { font-size: 14px; margin-bottom: 8px; color: #88f; }
  .test .config { margin-bottom: 8px; }
  .test label { font-size: 12px; color: #999; margin-right: 8px; }
  .test input { width: 60px; background: #333; border: 1px solid #555; color: #fff;
    padding: 2px 6px; border-radius: 4px; font-family: monospace; }
  .test button { background: #336; border: 1px solid #558; color: #aaf; padding: 4px 12px;
    border-radius: 4px; cursor: pointer; font-family: monospace; margin-right: 4px; }
  .test button:hover { background: #448; }
  .test button.stop { background: #633; border-color: #855; color: #faa; }
  .test .log { font-size: 12px; max-height: 200px; overflow-y: auto; margin-top: 8px;
    background: #111; padding: 8px; border-radius: 4px; white-space: pre-wrap; }
  .test .status { font-size: 12px; margin-top: 4px; }
  .status.connected { color: #8f8; }
  .status.disconnected { color: #f88; }
  .status.waiting { color: #ff8; }
</style>
</head>
<body>
<h1>Connection Timeout Test</h1>
<p class="nav"><a href="/suite">Automatisierte Suite starten</a></p>
<div class="tests">

  <div class="test">
    <h2>SSE (Server-Sent Events)</h2>
    <div class="config">
      <label>Pause (s):</label><input id="sse-pause" type="number" value="30">
      <label>Heartbeat (s):</label><input id="sse-hb" type="number" value="0">
      <button onclick="startSSE()">Start</button>
      <button class="stop" onclick="stopSSE()">Stop</button>
    </div>
    <div class="status" id="sse-status">-</div>
    <div class="log" id="sse-log"></div>
  </div>

  <div class="test">
    <h2>SSE Silent (kein Event nach Connect)</h2>
    <div class="config">
      <button onclick="startSSESilent()">Start</button>
      <button class="stop" onclick="stopSSESilent()">Stop</button>
    </div>
    <div class="status" id="sse-silent-status">-</div>
    <div class="log" id="sse-silent-log"></div>
  </div>

  <div class="test">
    <h2>WebSocket</h2>
    <div class="config">
      <label>Pause (s):</label><input id="ws-pause" type="number" value="30">
      <button onclick="startWS()">Start</button>
      <button class="stop" onclick="stopWS()">Stop</button>
    </div>
    <div class="status" id="ws-status">-</div>
    <div class="log" id="ws-log"></div>
  </div>

  <div class="test">
    <h2>HTTP Long Response</h2>
    <div class="config">
      <label>Delay (s):</label><input id="http-delay" type="number" value="90">
      <button onclick="startHTTP()">Start</button>
      <button class="stop" onclick="stopHTTP()">Stop</button>
    </div>
    <div class="status" id="http-status">-</div>
    <div class="log" id="http-log"></div>
  </div>

  <div class="test">
    <h2>Chunked Transfer</h2>
    <div class="config">
      <label>Pause (s):</label><input id="chunked-pause" type="number" value="30">
      <button onclick="startChunked()">Start</button>
      <button class="stop" onclick="stopChunked()">Stop</button>
    </div>
    <div class="status" id="chunked-status">-</div>
    <div class="log" id="chunked-log"></div>
  </div>

</div>

<script>
function ts() { return new Date().toLocaleTimeString(); }
function log(id, msg) {
  const el = document.getElementById(id);
  el.textContent += ts() + ' ' + msg + '\\n';
  el.scrollTop = el.scrollHeight;
}
function setStatus(id, text, cls) {
  const el = document.getElementById(id);
  el.textContent = text;
  el.className = 'status ' + cls;
}

let sseSource = null;
function startSSE() {
  stopSSE();
  const pause = document.getElementById('sse-pause').value;
  const hb = document.getElementById('sse-hb').value;
  document.getElementById('sse-log').textContent = '';
  sseSource = new EventSource('/sse?pause=' + pause + '&heartbeat=' + hb);
  const start = Date.now();
  setStatus('sse-status', 'Verbinde...', 'waiting');
  sseSource.onopen = () => { setStatus('sse-status', 'Verbunden', 'connected'); log('sse-log', 'Connected'); };
  sseSource.addEventListener('ping', (e) => { log('sse-log', '[' + ((Date.now()-start)/1000).toFixed(1) + 's] ' + e.data); });
  sseSource.addEventListener('heartbeat', () => { log('sse-log', '[' + ((Date.now()-start)/1000).toFixed(1) + 's] heartbeat'); });
  sseSource.onerror = () => { const e = ((Date.now()-start)/1000).toFixed(1); log('sse-log', '['+e+'s] LOST'); setStatus('sse-status', 'Abbruch nach '+e+'s', 'disconnected'); };
}
function stopSSE() { if (sseSource) { sseSource.close(); sseSource = null; } }

let sseSilentSource = null;
function startSSESilent() {
  stopSSESilent();
  document.getElementById('sse-silent-log').textContent = '';
  sseSilentSource = new EventSource('/sse-silent');
  const start = Date.now();
  setStatus('sse-silent-status', 'Verbinde...', 'waiting');
  sseSilentSource.onopen = () => { setStatus('sse-silent-status', 'Verbunden', 'connected'); log('sse-silent-log', 'Connected'); };
  sseSilentSource.onerror = () => { const e=((Date.now()-start)/1000).toFixed(1); log('sse-silent-log','['+e+'s] LOST'); setStatus('sse-silent-status','Abbruch nach '+e+'s','disconnected'); sseSilentSource.close(); sseSilentSource=null; };
}
function stopSSESilent() { if (sseSilentSource) { sseSilentSource.close(); sseSilentSource = null; } }

let wsConn = null;
function startWS() {
  stopWS();
  const pause = document.getElementById('ws-pause').value;
  document.getElementById('ws-log').textContent = '';
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  wsConn = new WebSocket(proto + '://' + location.host + '/ws?pause=' + pause);
  const start = Date.now();
  setStatus('ws-status', 'Verbinde...', 'waiting');
  wsConn.onopen = () => { setStatus('ws-status', 'Verbunden', 'connected'); log('ws-log', 'Connected'); };
  wsConn.onmessage = (e) => { log('ws-log', '[' + ((Date.now()-start)/1000).toFixed(1) + 's] ' + e.data); };
  wsConn.onclose = (e) => { const el=((Date.now()-start)/1000).toFixed(1); log('ws-log','['+el+'s] CLOSED code='+e.code); setStatus('ws-status','Geschlossen nach '+el+'s','disconnected'); };
  wsConn.onerror = () => { log('ws-log', 'ERROR'); };
}
function stopWS() { if (wsConn) { wsConn.close(); wsConn = null; } }

let httpCtrl = null;
function startHTTP() {
  stopHTTP();
  const delay = document.getElementById('http-delay').value;
  document.getElementById('http-log').textContent = '';
  httpCtrl = new AbortController();
  const start = Date.now();
  setStatus('http-status', 'Warte...', 'waiting');
  log('http-log', 'Request, warte ' + delay + 's...');
  fetch('/http?delay='+delay, {signal:httpCtrl.signal})
    .then(r=>r.text()).then(t=>{const e=((Date.now()-start)/1000).toFixed(1);log('http-log','['+e+'s] '+t);setStatus('http-status','OK nach '+e+'s','connected');})
    .catch(e=>{const el=((Date.now()-start)/1000).toFixed(1);log('http-log','['+el+'s] FAIL: '+e.message);setStatus('http-status','Fehler nach '+el+'s','disconnected');});
}
function stopHTTP() { if (httpCtrl) { httpCtrl.abort(); httpCtrl = null; } }

let chunkedCtrl = null;
function startChunked() {
  stopChunked();
  const pause = document.getElementById('chunked-pause').value;
  document.getElementById('chunked-log').textContent = '';
  chunkedCtrl = new AbortController();
  const start = Date.now();
  setStatus('chunked-status', 'Verbunden', 'connected');
  fetch('/chunked?pause='+pause, {signal:chunkedCtrl.signal})
    .then(async r=>{const reader=r.body.getReader();const d=new TextDecoder();for(;;){const{done,value}=await reader.read();if(done)break;log('chunked-log','['+((Date.now()-start)/1000).toFixed(1)+'s] '+d.decode(value).trim());}const e=((Date.now()-start)/1000).toFixed(1);log('chunked-log','['+e+'s] DONE');setStatus('chunked-status','Fertig nach '+e+'s','connected');})
    .catch(e=>{const el=((Date.now()-start)/1000).toFixed(1);log('chunked-log','['+el+'s] FAIL: '+e.message);setStatus('chunked-status','Abbruch nach '+el+'s','disconnected');});
}
function stopChunked() { if (chunkedCtrl) { chunkedCtrl.abort(); chunkedCtrl = null; } }
</script>
</body>
</html>`;

// ── Server ──────────────────────────────────────────────────────────
const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
  const url = new URL(req.url ?? "/", `http://localhost:${PORT}`);
  const path = url.pathname;
  const pause = Number(url.searchParams.get("pause") ?? "30");
  const heartbeat = Number(url.searchParams.get("heartbeat") ?? "0");
  const delay = Number(url.searchParams.get("delay") ?? "90");

  const now = () => new Date().toISOString();

  if (path === "/" || path === "/index.html") {
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(DASHBOARD_HTML);
    return;
  }

  if (path === "/suite") {
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(SUITE_HTML);
    return;
  }

  if (path === "/sse") {
    console.log(`[${now()}] SSE connected (pause=${pause}s, heartbeat=${heartbeat}s)`);
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });

    let alive = true;
    req.on("close", () => {
      alive = false;
      console.log(`[${now()}] SSE client disconnected`);
    });

    let hbInterval: ReturnType<typeof setInterval> | undefined;
    if (heartbeat > 0) {
      hbInterval = setInterval(() => {
        if (!alive) return;
        res.write("event: heartbeat\ndata: \n\n");
      }, heartbeat * 1000);
    }

    let count = 0;
    const sendEvent = () => {
      if (!alive) {
        if (hbInterval) clearInterval(hbInterval);
        return;
      }
      count++;
      console.log(`  [${now()}] SSE event ${count}`);
      res.write(`event: ping\ndata: Event ${count}\n\n`);
      setTimeout(sendEvent, pause * 1000);
    };

    res.write(`event: ping\ndata: Connected\n\n`);
    setTimeout(sendEvent, pause * 1000);
    return;
  }

  if (path === "/sse-silent") {
    console.log(`[${now()}] SSE-Silent connected`);
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    req.on("close", () => {
      console.log(`[${now()}] SSE-Silent client disconnected`);
    });
    return;
  }

  if (path === "/http") {
    console.log(`[${now()}] HTTP request (delay=${delay}s)`);
    let alive = true;
    req.on("close", () => {
      alive = false;
      console.log(`[${now()}] HTTP client disconnected before response`);
    });
    setTimeout(() => {
      if (!alive) return;
      console.log(`[${now()}] HTTP responding after ${delay}s`);
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end(`OK after ${delay}s`);
    }, delay * 1000);
    return;
  }

  if (path === "/chunked") {
    console.log(`[${now()}] Chunked connected (pause=${pause}s)`);
    res.writeHead(200, {
      "Content-Type": "text/plain",
      "Transfer-Encoding": "chunked",
      "X-Accel-Buffering": "no",
    });

    let alive = true;
    req.on("close", () => {
      alive = false;
      console.log(`[${now()}] Chunked client disconnected`);
    });

    let count = 0;
    const sendChunk = () => {
      if (!alive) return;
      count++;
      console.log(`  [${now()}] Chunk ${count}`);
      res.write(`Chunk ${count}\n`);
      if (count < 100) setTimeout(sendChunk, pause * 1000);
      else res.end();
    };

    res.write("Connected\n");
    setTimeout(sendChunk, pause * 1000);
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

// ── Minimal WebSocket (no dependencies) ─────────────────────────────
function wsFrame(data: string): Buffer {
  const payload = Buffer.from(data);
  const len = payload.length;
  let header: Buffer;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x81;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

server.on("upgrade", (req: IncomingMessage, socket, head) => {
  const url = new URL(req.url ?? "/", `http://localhost:${PORT}`);
  if (url.pathname !== "/ws") {
    socket.destroy();
    return;
  }

  const pause = Number(url.searchParams.get("pause") ?? "30");
  const now = () => new Date().toISOString();
  console.log(`[${now()}] WebSocket connected (pause=${pause}s)`);

  const key = req.headers["sec-websocket-key"] ?? "";
  const accept = createHash("sha1")
    .update(key + "258EAFA5-E914-47DA-95CA-5AB5DC175D22")
    .digest("base64");

  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
    "Upgrade: websocket\r\n" +
    "Connection: Upgrade\r\n" +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );

  let alive = true;
  let count = 0;

  socket.on("close", () => {
    alive = false;
    console.log(`[${now()}] WebSocket client disconnected`);
  });
  socket.on("error", () => {
    alive = false;
  });

  socket.write(wsFrame("Connected"));

  const sendMsg = () => {
    if (!alive) return;
    count++;
    console.log(`  [${now()}] WS message ${count}`);
    socket.write(wsFrame(`Message ${count}`));
    setTimeout(sendMsg, pause * 1000);
  };

  setTimeout(sendMsg, pause * 1000);
});

server.listen(PORT, () => {
  console.log(`
Connection Timeout Test
=======================
http://localhost:${PORT}         Manual Dashboard
http://localhost:${PORT}/suite   Automated Test Suite
`);
});
