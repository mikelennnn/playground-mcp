/**
 * Playground MCP — servidor REMOTO (para usar Claude desde el celular)
 * --------------------------------------------------------------------
 *   App de Claude ──(MCP por HTTPS)──► este servidor ──(WebSocket wss)──► tu pestaña de Mi Playground
 *
 * Rutas:
 *   POST /mcp/<PLAYGROUND_TOKEN>   endpoint MCP (Streamable HTTP, respuestas JSON)
 *   POST /mcp + "Authorization: Bearer <PLAYGROUND_TOKEN>"   (alternativa)
 *   WSS  /bridge                   puente al que se conecta la pestaña del Playground
 *   GET  /                         página de estado (no muestra secretos)
 *
 * Variables de entorno (en Render → Environment):
 *   PLAYGROUND_TOKEN            OBLIGATORIA. Código secreto largo. Protege el MCP y el puente.
 *   PLAYGROUND_ALLOWED_ORIGINS  Opcional. Ej: https://mi-playground.vercel.app
 *   PORT                        La pone Render automáticamente.
 *
 * Sin dependencias: solo Node.js 18+.
 */
import http from 'node:http';
import crypto from 'node:crypto';

const VERSION = '1.1.0';
const PORT = Number(process.env.PORT || 8787);
const TOKEN = process.env.PLAYGROUND_TOKEN || '';
const ALLOWED = (process.env.PLAYGROUND_ALLOWED_ORIGINS || '').split(',').map(s => s.trim().replace(/\/$/, '')).filter(Boolean);
const CALL_TIMEOUT_MS = 25000;
const MAX_BODY = 8 * 1024 * 1024;

const log = (...a) => console.log(new Date().toISOString(), '[playground-mcp]', ...a);

if (TOKEN.length < 12) {
  console.error('\n✖ Falta PLAYGROUND_TOKEN (mínimo 12 caracteres). Agrégala en Environment y vuelve a desplegar.\n');
  process.exit(1);
}
function safeEqual(a, b) {
  const x = crypto.createHash('sha256').update(String(a)).digest();
  const y = crypto.createHash('sha256').update(String(b)).digest();
  return crypto.timingSafeEqual(x, y) && String(a) === String(b);
}

/* ============================================================
   Puente WebSocket (RFC 6455 mínimo)
   ============================================================ */
let client = null;
const pending = new Map();

function encodeFrame(data, opcode = 0x1) {
  const payload = Buffer.isBuffer(data) ? data : Buffer.from(data);
  const len = payload.length; let header;
  if (len < 126) header = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { header = Buffer.alloc(4); header[0] = 0x80 | opcode; header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x80 | opcode; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([header, payload]);
}

function attachSocket(socket) {
  let buf = Buffer.alloc(0), fragments = [], authed = false, pingTimer = null, lastSeen = Date.now();
  const conn = {
    send: obj => { if (!socket.destroyed) socket.write(encodeFrame(JSON.stringify(obj))); },
    close: (code = 1000) => { try { const b = Buffer.alloc(2); b.writeUInt16BE(code); socket.write(encodeFrame(b, 0x8)); } catch {} socket.end(); setTimeout(() => socket.destroy(), 1000); }
  };
  const helloTimer = setTimeout(() => { if (!authed) conn.close(1008); }, 15000);

  socket.on('data', chunk => {
    lastSeen = Date.now();
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const fin = (buf[0] & 0x80) !== 0, opcode = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_BODY) return conn.close(1009);
      if (!masked) return conn.close(1002);
      if (buf.length < off + 4 + len) return;
      const mask = buf.subarray(off, off + 4);
      const payload = Buffer.from(buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      buf = buf.subarray(off + 4 + len);
      if (opcode === 0x8) return conn.close();
      if (opcode === 0x9) { socket.write(encodeFrame(payload, 0xA)); continue; }
      if (opcode === 0xA) continue;
      if (opcode === 0x1 || opcode === 0x0) {
        fragments.push(payload); if (!fin) continue;
        const text = Buffer.concat(fragments).toString('utf8'); fragments = [];
        let msg; try { msg = JSON.parse(text); } catch { continue; }
        onMessage(msg);
      }
    }
  });

  function onMessage(msg) {
    if (!authed) {
      if (msg.type !== 'hello') return;
      if (!safeEqual(msg.token || '', TOKEN)) { log('Puente: código incorrecto'); conn.send({ type: 'denied', message: 'Código incorrecto' }); return setTimeout(() => conn.close(1008), 50); }
      authed = true; clearTimeout(helloTimer);
      if (client && client !== conn) client.close(4000);
      client = conn;
      conn.send({ type: 'welcome', server: 'Playground MCP (remoto)', version: VERSION });
      // ping cada 20 s (mantiene viva la conexión a través del proxy) y detecta pestañas dormidas
      pingTimer = setInterval(() => {
        if (Date.now() - lastSeen > 70000) { log('Puente: la pestaña dejó de responder'); return conn.close(4001); }
        conn.send({ type: 'ping' });
      }, 20000);
      return log('Puente: pestaña conectada');
    }
    if (msg.type === 'result' && pending.has(msg.id)) {
      const p = pending.get(msg.id); pending.delete(msg.id); clearTimeout(p.timer);
      msg.ok ? p.resolve(msg.result) : p.reject(new Error(msg.error || 'Error en el Playground'));
    }
  }
  const cleanup = () => {
    clearTimeout(helloTimer); clearInterval(pingTimer);
    if (client === conn) {
      client = null; log('Puente: pestaña desconectada');
      for (const [id, p] of pending) { clearTimeout(p.timer); p.reject(new Error('La pestaña del Playground se desconectó')); pending.delete(id); }
    }
  };
  socket.on('close', cleanup); socket.on('error', cleanup);
}

function callBrowser(method, params = {}) {
  if (!client) return Promise.reject(new Error(
    'El Playground no está conectado. Pide al usuario que abra Mi Playground en Chrome, toque "Claude MCP", ' +
    'escriba la dirección wss://…/bridge y su código, y pulse "Conectar". En Android conviene usar pantalla dividida para que la pestaña siga activa.'));
  const id = crypto.randomUUID();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`El Playground no respondió a ${method} (¿la pestaña está en segundo plano?)`)); }, CALL_TIMEOUT_MS);
    pending.set(id, { resolve, reject, timer });
    client.send({ type: 'call', id, method, params });
  });
}

/* ============================================================
   Herramientas MCP
   ============================================================ */
const FILE_ENUM = { type: 'string', enum: ['html', 'css', 'js'], description: 'Archivo del proyecto' };
const EMPTY = { type: 'object', properties: {} };
const TOOLS = [
  { name: 'playground_status', description: 'Indica si la pestaña de Mi Playground está conectada y cómo conectarla.', inputSchema: EMPTY, local: true },
  { name: 'get_project', method: 'getProject', description: 'Proyecto abierto en Mi Playground: id, nombre, fechas, cambios sin guardar y archivos html, css y js.', inputSchema: EMPTY },
  { name: 'get_files', method: 'getFiles', description: 'Contenido de los tres archivos (html, css, js) del proyecto abierto.', inputSchema: EMPTY },
  { name: 'get_file', method: 'getFile', description: 'Lee un archivo del proyecto abierto.', inputSchema: { type: 'object', properties: { name: FILE_ENUM }, required: ['name'] } },
  { name: 'set_file', method: 'setFile', description: 'Reemplaza por completo un archivo (html = contenido del <body>, css = estilos, js = script). Envía siempre el archivo completo. Luego usa run_code para actualizar la vista y ver errores.', inputSchema: { type: 'object', properties: { name: FILE_ENUM, content: { type: 'string', description: 'Contenido completo del archivo' } }, required: ['name', 'content'] } },
  { name: 'create_project', method: 'createProject', description: 'Crea un proyecto vacío con ese nombre y lo abre (el actual se guarda antes).', inputSchema: { type: 'object', properties: { name: { type: 'string' } }, required: ['name'] } },
  { name: 'save_project', method: 'saveProject', description: 'Guarda el proyecto abierto en el navegador.', inputSchema: EMPTY },
  { name: 'run_code', method: 'runCode', description: 'Ejecuta el código y recarga la vista previa; devuelve duración y número de errores. Después usa get_console.', inputSchema: EMPTY },
  { name: 'get_console', method: 'getConsole', description: 'Mensajes de consola capturados (log, warn, error, errores de JavaScript).', inputSchema: EMPTY },
  { name: 'clear_console', method: 'clearConsole', description: 'Limpia la consola del Playground.', inputSchema: EMPTY }
];

async function rpc(msg) {
  const { id, method, params = {} } = msg || {};
  const isReq = id !== undefined && id !== null;
  const ok = result => ({ jsonrpc: '2.0', id, result });
  const err = (code, message) => ({ jsonrpc: '2.0', id: isReq ? id : null, error: { code, message } });
  switch (method) {
    case 'initialize':
      return ok({
        protocolVersion: params.protocolVersion || '2025-06-18',
        capabilities: { tools: { listChanged: false } },
        serverInfo: { name: 'playground-mcp', title: 'Playground MCP', version: VERSION },
        instructions: 'Controla "Mi Playground" (editor HTML/CSS/JS en el navegador del usuario). Flujo: get_files → set_file (archivo completo) → run_code → get_console → save_project. Si dice que no está conectado, explica cómo conectarlo (playground_status).'
      });
    case 'ping': return ok({});
    case 'tools/list': return ok({ tools: TOOLS.map(({ name, description, inputSchema }) => ({ name, description, inputSchema })) });
    case 'tools/call': {
      const tool = TOOLS.find(t => t.name === params.name);
      if (!tool) return err(-32602, `Herramienta desconocida: ${params.name}`);
      try {
        const result = tool.local
          ? { browserConnected: !!client, bridgePath: '/bridge', howToConnect: client ? undefined : 'En Mi Playground: "Claude MCP" → dirección wss://<este servidor>/bridge → código → Conectar.' }
          : await callBrowser(tool.method, params.arguments || {});
        return ok({ content: [{ type: 'text', text: JSON.stringify(result ?? { ok: true }, null, 2) }], isError: false });
      } catch (e) {
        return ok({ content: [{ type: 'text', text: 'Error: ' + e.message }], isError: true });
      }
    }
    default:
      if (!isReq) return null; // notificaciones (notifications/initialized, etc.)
      return err(-32601, `Método no soportado: ${method}`);
  }
}

/* ============================================================
   HTTP
   ============================================================ */
function readBody(req) {
  return new Promise((res, rej) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > MAX_BODY) { rej(new Error('too large')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => res(Buffer.concat(chunks).toString('utf8'))); req.on('error', rej);
  });
}
function authorized(req, path) {
  const m = /^\/mcp\/([^/?#]+)\/?$/.exec(path);
  if (m) return safeEqual(decodeURIComponent(m[1]), TOKEN);
  if (path === '/mcp' || path === '/mcp/') {
    const h = req.headers.authorization || '';
    return h.startsWith('Bearer ') && safeEqual(h.slice(7).trim(), TOKEN);
  }
  return false;
}

const server = http.createServer(async (req, res) => {
  const path = (req.url || '/').split('?')[0];
  const send = (code, body, type = 'application/json') => { res.writeHead(code, { 'Content-Type': type + '; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(body); };

  if (path === '/' && req.method === 'GET') {
    return send(200, `Playground MCP ${VERSION} — en línea.\nPestaña del Playground: ${client ? 'CONECTADA' : 'no conectada'}\n`, 'text/plain');
  }
  if (path === '/health') return send(200, '{"ok":true}');

  if (path.startsWith('/mcp')) {
    if (!authorized(req, path)) return send(404, '{"error":"not found"}');
    if (req.method === 'GET') { res.writeHead(405, { Allow: 'POST' }); return res.end(); }
    if (req.method === 'DELETE') { res.writeHead(200); return res.end(); }
    if (req.method !== 'POST') { res.writeHead(405, { Allow: 'POST' }); return res.end(); }
    let body;
    try { body = JSON.parse(await readBody(req)); }
    catch { return send(400, JSON.stringify({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'JSON inválido' } })); }
    const batch = Array.isArray(body);
    const results = (await Promise.all((batch ? body : [body]).map(rpc))).filter(Boolean);
    if (!results.length) { res.writeHead(202); return res.end(); }
    return send(200, JSON.stringify(batch ? results : results[0]));
  }
  // rutas de descubrimiento OAuth: este servidor no usa OAuth (el secreto va en la URL)
  return send(404, '{"error":"not found"}');
});

server.on('upgrade', (req, socket) => {
  const path = (req.url || '').split('?')[0];
  const origin = (req.headers.origin || 'null').replace(/\/$/, '');
  const key = req.headers['sec-websocket-key'];
  if (path !== '/bridge' || !key) { socket.end('HTTP/1.1 404 Not Found\r\n\r\n'); return; }
  if (ALLOWED.length && !ALLOWED.includes(origin)) { log('Puente: origen rechazado', origin); socket.end('HTTP/1.1 403 Forbidden\r\n\r\n'); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write(['HTTP/1.1 101 Switching Protocols', 'Upgrade: websocket', 'Connection: Upgrade', `Sec-WebSocket-Accept: ${accept}`, '', ''].join('\r\n'));
  socket.setNoDelay(true); socket.setKeepAlive(true, 30000);
  attachSocket(socket);
});

server.listen(PORT, '0.0.0.0', () => log(`Escuchando en el puerto ${PORT}. MCP: /mcp/<código>  ·  Puente: /bridge`));
