#!/usr/bin/env npx tsx
/**
 * Connection Timeout Test
 *
 * Standalone server to identify timeout limits on any hosting platform.
 * Tests SSE, WebSocket, and long-running HTTP responses.
 *
 * Usage:
 *   npx tsx connection-timeout-test.ts [--port 3333]
 *
 * Then open http://<host>:3333 in the browser for the test dashboard,
 * or test individual endpoints:
 *
 *   # SSE with configurable pause (seconds between events)
 *   curl -N http://<host>:3333/sse?pause=30
 *   curl -N http://<host>:3333/sse?pause=60
 *   curl -N http://<host>:3333/sse?pause=120
 *
 *   # SSE with heartbeat (pause between real events, heartbeat interval)
 *   curl -N "http://<host>:3333/sse?pause=120&heartbeat=15"
 *
 *   # SSE completely silent (no data at all after connect)
 *   curl -N http://<host>:3333/sse-silent
 *
 *   # WebSocket with configurable pause
 *   wscat -c ws://<host>:3333/ws?pause=30
 *
 *   # Long-running HTTP (waits N seconds before responding)
 *   curl http://<host>:3333/http?delay=60
 *
 *   # Chunked transfer with pauses between chunks
 *   curl -N http://<host>:3333/chunked?pause=30
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

// ── Dashboard HTML ──────────────────────────────────────────────────
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8">
<title>Connection Timeout Test</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: monospace; background: #1a1a1a; color: #e0e0e0; padding: 20px; }
  h1 { margin-bottom: 20px; }
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
<div class="tests">

  <!-- SSE -->
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

  <!-- SSE Silent -->
  <div class="test">
    <h2>SSE Silent (kein Event nach Connect)</h2>
    <div class="config">
      <button onclick="startSSESilent()">Start</button>
      <button class="stop" onclick="stopSSESilent()">Stop</button>
    </div>
    <div class="status" id="sse-silent-status">-</div>
    <div class="log" id="sse-silent-log"></div>
  </div>

  <!-- WebSocket -->
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

  <!-- HTTP Long Poll -->
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

  <!-- Chunked -->
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

// SSE
let sseSource = null;
function startSSE() {
  stopSSE();
  const pause = document.getElementById('sse-pause').value;
  const hb = document.getElementById('sse-hb').value;
  document.getElementById('sse-log').textContent = '';
  const url = '/sse?pause=' + pause + '&heartbeat=' + hb;
  sseSource = new EventSource(url);
  const start = Date.now();
  setStatus('sse-status', 'Verbinde...', 'waiting');
  sseSource.onopen = () => {
    setStatus('sse-status', 'Verbunden', 'connected');
    log('sse-log', 'Connected');
  };
  sseSource.addEventListener('ping', (e) => {
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    log('sse-log', '[' + elapsed + 's] event: ' + e.data);
  });
  sseSource.addEventListener('heartbeat', () => {
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    log('sse-log', '[' + elapsed + 's] heartbeat');
  });
  sseSource.addEventListener('done', (e) => {
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    log('sse-log', '[' + elapsed + 's] DONE: ' + e.data);
    sseSource.close();
    setStatus('sse-status', 'Fertig nach ' + elapsed + 's', 'connected');
  });
  sseSource.onerror = () => {
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    log('sse-log', '[' + elapsed + 's] CONNECTION LOST');
    setStatus('sse-status', 'Abgebrochen nach ' + elapsed + 's', 'disconnected');
  };
}
function stopSSE() { if (sseSource) { sseSource.close(); sseSource = null; } }

// SSE Silent
let sseSilentSource = null;
function startSSESilent() {
  stopSSESilent();
  document.getElementById('sse-silent-log').textContent = '';
  sseSilentSource = new EventSource('/sse-silent');
  const start = Date.now();
  setStatus('sse-silent-status', 'Verbinde...', 'waiting');
  sseSilentSource.onopen = () => {
    setStatus('sse-silent-status', 'Verbunden (warte auf Timeout...)', 'connected');
    log('sse-silent-log', 'Connected — waiting for timeout');
  };
  sseSilentSource.onerror = () => {
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    log('sse-silent-log', '[' + elapsed + 's] CONNECTION LOST');
    setStatus('sse-silent-status', 'Abgebrochen nach ' + elapsed + 's', 'disconnected');
    sseSilentSource.close();
    sseSilentSource = null;
  };
}
function stopSSESilent() { if (sseSilentSource) { sseSilentSource.close(); sseSilentSource = null; } }

// WebSocket
let wsConn = null;
function startWS() {
  stopWS();
  const pause = document.getElementById('ws-pause').value;
  document.getElementById('ws-log').textContent = '';
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  wsConn = new WebSocket(proto + '://' + location.host + '/ws?pause=' + pause);
  const start = Date.now();
  setStatus('ws-status', 'Verbinde...', 'waiting');
  wsConn.onopen = () => {
    setStatus('ws-status', 'Verbunden', 'connected');
    log('ws-log', 'Connected');
  };
  wsConn.onmessage = (e) => {
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    log('ws-log', '[' + elapsed + 's] ' + e.data);
  };
  wsConn.onclose = (e) => {
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    log('ws-log', '[' + elapsed + 's] CLOSED (code=' + e.code + ')');
    setStatus('ws-status', 'Geschlossen nach ' + elapsed + 's', 'disconnected');
  };
  wsConn.onerror = () => {
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    log('ws-log', '[' + elapsed + 's] ERROR');
  };
}
function stopWS() { if (wsConn) { wsConn.close(); wsConn = null; } }

// HTTP
let httpController = null;
function startHTTP() {
  stopHTTP();
  const delay = document.getElementById('http-delay').value;
  document.getElementById('http-log').textContent = '';
  httpController = new AbortController();
  const start = Date.now();
  setStatus('http-status', 'Warte auf Antwort...', 'waiting');
  log('http-log', 'Request gestartet, warte ' + delay + 's...');
  fetch('/http?delay=' + delay, { signal: httpController.signal })
    .then(r => r.text())
    .then(text => {
      const elapsed = ((Date.now() - start) / 1000).toFixed(1);
      log('http-log', '[' + elapsed + 's] Response: ' + text);
      setStatus('http-status', 'Antwort nach ' + elapsed + 's', 'connected');
    })
    .catch(e => {
      const elapsed = ((Date.now() - start) / 1000).toFixed(1);
      log('http-log', '[' + elapsed + 's] FAILED: ' + e.message);
      setStatus('http-status', 'Fehler nach ' + elapsed + 's', 'disconnected');
    });
}
function stopHTTP() { if (httpController) { httpController.abort(); httpController = null; } }

// Chunked
let chunkedController = null;
function startChunked() {
  stopChunked();
  const pause = document.getElementById('chunked-pause').value;
  document.getElementById('chunked-log').textContent = '';
  chunkedController = new AbortController();
  const start = Date.now();
  setStatus('chunked-status', 'Verbunden', 'connected');
  log('chunked-log', 'Connected');
  fetch('/chunked?pause=' + pause, { signal: chunkedController.signal })
    .then(async r => {
      const reader = r.body.getReader();
      const decoder = new TextDecoder();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        const elapsed = ((Date.now() - start) / 1000).toFixed(1);
        log('chunked-log', '[' + elapsed + 's] chunk: ' + decoder.decode(value).trim());
      }
      const elapsed = ((Date.now() - start) / 1000).toFixed(1);
      log('chunked-log', '[' + elapsed + 's] DONE');
      setStatus('chunked-status', 'Fertig nach ' + elapsed + 's', 'connected');
    })
    .catch(e => {
      const elapsed = ((Date.now() - start) / 1000).toFixed(1);
      log('chunked-log', '[' + elapsed + 's] FAILED: ' + e.message);
      setStatus('chunked-status', 'Abgebrochen nach ' + elapsed + 's', 'disconnected');
    });
}
function stopChunked() { if (chunkedController) { chunkedController.abort(); chunkedController = null; } }
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

  // Dashboard
  if (path === "/" || path === "/index.html") {
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(DASHBOARD_HTML);
    return;
  }

  // SSE
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

    // Send first event immediately
    res.write(`event: ping\ndata: Connected\n\n`);
    setTimeout(sendEvent, pause * 1000);
    return;
  }

  // SSE Silent
  if (path === "/sse-silent") {
    console.log(`[${now()}] SSE-Silent connected`);
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    // Send nothing, just hold the connection
    req.on("close", () => {
      console.log(`[${now()}] SSE-Silent client disconnected`);
    });
    return;
  }

  // HTTP long response
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

  // Chunked
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
    header[0] = 0x81; // fin + text
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

  // Perform WebSocket handshake
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
http://localhost:${PORT}

Endpoints:
  GET  /             Dashboard (Browser)
  GET  /sse           SSE stream (?pause=30&heartbeat=0)
  GET  /sse-silent    SSE ohne Events
  GET  /ws            WebSocket (?pause=30)
  GET  /http          Long HTTP response (?delay=90)
  GET  /chunked       Chunked transfer (?pause=30)
`);
});
