/**
 * Playground MCP v2 — un solo archivo, sin dependencias (Node.js 18+)
 * ------------------------------------------------------------------
 *   Claude ──(MCP)──► este servidor ──(WebSocket)──► pestaña de Mi Playground
 *
 * Dos modos:
 *   • Remoto (Render, para usar Claude desde el celular):   node server.mjs
 *       POST /mcp/<PLAYGROUND_TOKEN>     endpoint MCP (Streamable HTTP, respuestas JSON)
 *       WSS  /bridge                     puente al que se conecta el Playground
 *   • Local (Claude Code / Claude Desktop):                  node server.mjs --stdio
 *       MCP por stdio + puente en ws://127.0.0.1:8787/bridge
 *
 * Variables de entorno:
 *   PLAYGROUND_TOKEN            OBLIGATORIA. Código secreto (mínimo 12 caracteres).
 *   PLAYGROUND_ALLOWED_ORIGINS  Opcional. Ej: https://mi-playground.vercel.app
 *   PORT / PLAYGROUND_PORT      Puerto (Render pone PORT automáticamente; local 8787).
 *
 * Ahorro de tokens: las herramientas devuelven texto compacto (no JSON con escapes),
 * permiten leer por rangos, buscar, editar fragmentos y ejecutar en la misma llamada.
 */
import http from 'node:http';
import crypto from 'node:crypto';
import readline from 'node:readline';

const VERSION = '2.0.0';
const STDIO = process.argv.includes('--stdio') || process.env.MCP_STDIO === '1';
const PORT = Number(process.env.PORT || process.env.PLAYGROUND_PORT || 8787);
const HOST = STDIO ? '127.0.0.1' : '0.0.0.0';
const TOKEN = process.env.PLAYGROUND_TOKEN || '';
const ALLOWED = (process.env.PLAYGROUND_ALLOWED_ORIGINS || '').split(',').map(s => s.trim().replace(/\/$/, '')).filter(Boolean);
const CALL_TIMEOUT_MS = 25000;
const MAX_BODY = 8 * 1024 * 1024;

// En modo stdio, stdout es el canal MCP: todo registro va a stderr.
const log = (...a) => process.stderr.write(`${new Date().toISOString()} [playground-mcp] ${a.join(' ')}\n`);

if (TOKEN.length < 12) {
  log('✖ Falta PLAYGROUND_TOKEN (mínimo 12 caracteres). Agrégala y vuelve a iniciar.');
  process.exit(1);
}
const safeEqual = (a, b) => {
  const x = crypto.createHash('sha256').update(String(a)).digest(), y = crypto.createHash('sha256').update(String(b)).digest();
  return crypto.timingSafeEqual(x, y) && String(a) === String(b);
};

/* ============================================================ Puente WebSocket (RFC 6455 mínimo) */
let client = null;          // { send, close, apiVersion }
const pending = new Map();

function frame(data, opcode = 0x1) {
  const p = Buffer.isBuffer(data) ? data : Buffer.from(data); const n = p.length; let h;
  if (n < 126) h = Buffer.from([0x80 | opcode, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | opcode; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | opcode; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, p]);
}
function attachSocket(socket) {
  let buf = Buffer.alloc(0), frags = [], authed = false, pingTimer = null, lastSeen = Date.now();
  const conn = {
    apiVersion: '0',
    send: o => { if (!socket.destroyed) socket.write(frame(JSON.stringify(o))); },
    close: (code = 1000) => { try { const b = Buffer.alloc(2); b.writeUInt16BE(code); socket.write(frame(b, 0x8)); } catch {} socket.end(); setTimeout(() => socket.destroy(), 1000); }
  };
  const helloTimer = setTimeout(() => { if (!authed) conn.close(1008); }, 15000);
  socket.on('data', chunk => {
    lastSeen = Date.now(); buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const fin = (buf[0] & 0x80) !== 0, op = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_BODY) return conn.close(1009);
      if (!masked) return conn.close(1002);
      if (buf.length < off + 4 + len) return;
      const mask = buf.subarray(off, off + 4); const pl = Buffer.from(buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < pl.length; i++) pl[i] ^= mask[i & 3];
      buf = buf.subarray(off + 4 + len);
      if (op === 0x8) return conn.close();
      if (op === 0x9) { socket.write(frame(pl, 0xA)); continue; }
      if (op === 0xA) continue;
      if (op === 0x1 || op === 0x0) {
        frags.push(pl); if (!fin) continue;
        const text = Buffer.concat(frags).toString('utf8'); frags = [];
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
      conn.apiVersion = String(msg.apiVersion || '1');
      if (client && client !== conn) client.close(4000);
      client = conn;
      conn.send({ type: 'welcome', server: 'Playground MCP', version: VERSION });
      pingTimer = setInterval(() => { if (Date.now() - lastSeen > 70000) { log('Puente: la pestaña dejó de responder'); return conn.close(4001); } conn.send({ type: 'ping' }); }, 20000);
      return log(`Puente: pestaña conectada (Playground v${conn.apiVersion})`);
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
  if (!client) return Promise.reject(new Error('El Playground no está conectado. Pide al usuario: abrir Mi Playground → "Claude MCP" → dirección …/bridge + código → Conectar. En Android, pantalla dividida para que la pestaña siga activa.'));
  if (+client.apiVersion.split('.')[0] < 2) return Promise.reject(new Error(`La pestaña usa Mi Playground v${client.apiVersion}; este servidor necesita v2. Pide al usuario actualizar index.html.`));
  const id = crypto.randomUUID();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`El Playground no respondió a ${method} (¿pestaña en segundo plano?)`)); }, CALL_TIMEOUT_MS);
    pending.set(id, { resolve, reject, timer });
    client.send({ type: 'call', id, method, params });
  });
}

/* ============================================================ Herramientas (descripciones cortas = menos tokens) */
const S = (properties = {}, required = []) => ({ type: 'object', properties, required, additionalProperties: false });
const PATH = { type: 'string', description: 'Ruta relativa, ej. js/main.js' };
const RUN = { type: 'boolean', description: 'Ejecutar después y devolver errores/consola' };
const TOOLS = [
  { name: 'playground_status', description: 'Si la pestaña de Mi Playground está conectada.', inputSchema: S(), local: true },
  { name: 'projects', method: 'projects', description: 'Proyectos: list | open (id o name) | create (name) | rename (name) | save. open/create devuelven el árbol.',
    inputSchema: S({ action: { type: 'string', enum: ['list', 'open', 'create', 'rename', 'save'] }, id: { type: 'string' }, name: { type: 'string' } }, ['action']) },
  { name: 'tree', method: 'tree', description: 'Árbol del proyecto abierto: carpetas, archivos y número de líneas (sin contenido).', inputSchema: S() },
  { name: 'read_file', method: 'readFile', description: 'Lee un archivo (máx. 600 líneas por llamada). Usa start/end para rangos; numbered=true agrega "N|" a cada línea.',
    inputSchema: S({ path: PATH, start: { type: 'integer', minimum: 1 }, end: { type: 'integer', minimum: 1 }, numbered: { type: 'boolean' } }, ['path']) },
  { name: 'write_file', method: 'writeFile', description: 'Crea un archivo (y sus carpetas) o lo reescribe completo. Para cambios en archivos existentes prefiere edit_file.',
    inputSchema: S({ path: PATH, content: { type: 'string' }, run: RUN }, ['path', 'content']) },
  { name: 'edit_file', method: 'editFile', description: 'Edición parcial, sin reenviar el archivo. Cada edición: {old_text,new_text[,replace_all]} (old_text exacto y único; new_text "" borra) o {start_line,end_line,new_text} (para insertar sin borrar: end_line = start_line-1). Se aplican en orden; si una falla no se aplica ninguna y el error sugiere líneas parecidas.',
    inputSchema: S({ path: PATH, edits: { type: 'array', minItems: 1, items: { type: 'object', properties: { old_text: { type: 'string' }, new_text: { type: 'string' }, replace_all: { type: 'boolean' }, start_line: { type: 'integer' }, end_line: { type: 'integer' } }, required: ['new_text'] } }, run: RUN }, ['path', 'edits']) },
  { name: 'file_op', method: 'fileOp', description: 'mkdir (carpeta) | move (renombrar/mover archivo o carpeta, usa to) | copy (archivo, usa to) | delete (archivo o carpeta completa).',
    inputSchema: S({ op: { type: 'string', enum: ['mkdir', 'move', 'copy', 'delete'] }, path: PATH, to: { type: 'string' }, run: RUN }, ['op', 'path']) },
  { name: 'search', method: 'search', description: 'Busca en todos los archivos de texto; devuelve "ruta:línea: texto" (máx. 40). Úsalo para ubicar código sin leer archivos completos.',
    inputSchema: S({ query: { type: 'string' }, regex: { type: 'boolean' }, path: { type: 'string', description: 'Limitar a una carpeta o archivo' } }, ['query']) },
  { name: 'run', method: 'run', description: 'Ejecuta el proyecto, espera a que cargue y devuelve duración, errores (con archivo:línea) y la consola de esa ejecución.', inputSchema: S() },
  { name: 'console', method: 'console', description: 'Últimos mensajes de la consola (por defecto 30). clear=true la limpia.',
    inputSchema: S({ limit: { type: 'integer', minimum: 1, maximum: 100 }, clear: { type: 'boolean' } }) }
];
const INSTRUCTIONS = 'Controla "Mi Playground" (proyectos web con carpetas en el navegador del usuario; entrada index.html; <link>/<script src> locales, módulos ES, imágenes y fetch() de archivos del proyecto funcionan). Ahorra tokens: tree → search/read_file con rangos → edit_file (no reescribas archivos completos para cambios pequeños) → pasa run:true en la edición final para ver errores en la misma llamada. Se guarda solo.';

async function callTool(name, args = {}) {
  const t = TOOLS.find(x => x.name === name);
  if (!t) throw Object.assign(new Error(`Herramienta desconocida: ${name}`), { rpc: -32602 });
  if (t.local) return client ? `Conectado (Playground v${client.apiVersion}).` : 'No conectado. El usuario debe abrir Mi Playground → "Claude MCP" → dirección …/bridge + código → Conectar.';
  const r = await callBrowser(t.method, args);
  return r && typeof r.text === 'string' ? r.text : JSON.stringify(r ?? { ok: true });
}
async function rpc(msg) {
  const { id, method, params = {} } = msg || {};
  const isReq = id !== undefined && id !== null;
  const ok = result => ({ jsonrpc: '2.0', id, result });
  const err = (code, message) => ({ jsonrpc: '2.0', id: isReq ? id : null, error: { code, message } });
  switch (method) {
    case 'initialize':
      return ok({ protocolVersion: params.protocolVersion || '2025-06-18', capabilities: { tools: { listChanged: false } }, serverInfo: { name: 'playground-mcp', title: 'Playground MCP', version: VERSION }, instructions: INSTRUCTIONS });
    case 'ping': return ok({});
    case 'tools/list': return ok({ tools: TOOLS.map(({ name, description, inputSchema }) => ({ name, description, inputSchema })) });
    case 'tools/call':
      try { return ok({ content: [{ type: 'text', text: await callTool(params.name, params.arguments || {}) }], isError: false }); }
      catch (e) { return e.rpc ? err(e.rpc, e.message) : ok({ content: [{ type: 'text', text: 'Error: ' + e.message }], isError: true }); }
    default:
      return isReq ? err(-32601, `Método no soportado: ${method}`) : null; // notificaciones
  }
}

/* ============================================================ HTTP + puente */
function readBody(req) {
  return new Promise((res, rej) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > MAX_BODY) { rej(new Error('too large')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => res(Buffer.concat(chunks).toString('utf8'))); req.on('error', rej);
  });
}
function authorized(req, path) {
  const m = /^\/mcp\/([^/?#]+)\/?$/.exec(path);
  if (m) { try { return safeEqual(decodeURIComponent(m[1]), TOKEN); } catch { return false; } }
  if (path === '/mcp' || path === '/mcp/') { const h = req.headers.authorization || ''; return h.startsWith('Bearer ') && safeEqual(h.slice(7).trim(), TOKEN); }
  return false;
}
const server = http.createServer(async (req, res) => {
  const path = (req.url || '/').split('?')[0];
  const send = (code, body, type = 'application/json') => { res.writeHead(code, { 'Content-Type': type + '; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(body); };
  if (path === '/' && req.method === 'GET') return send(200, `Playground MCP ${VERSION} — en línea.\nPestaña del Playground: ${client ? 'CONECTADA' : 'no conectada'}\n`, 'text/plain');
  if (path === '/health') return send(200, '{"ok":true}');
  if (!STDIO && path.startsWith('/mcp')) {
    if (!authorized(req, path)) return send(404, '{"error":"not found"}');
    if (req.method === 'GET') { res.writeHead(405, { Allow: 'POST' }); return res.end(); }
    if (req.method === 'DELETE') { res.writeHead(200); return res.end(); }
    if (req.method !== 'POST') { res.writeHead(405, { Allow: 'POST' }); return res.end(); }
    let body;
    try { body = JSON.parse(await readBody(req)); } catch { return send(400, JSON.stringify({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'JSON inválido' } })); }
    const batch = Array.isArray(body);
    const out = (await Promise.all((batch ? body : [body]).map(rpc))).filter(Boolean);
    if (!out.length) { res.writeHead(202); return res.end(); }
    return send(200, JSON.stringify(batch ? out : out[0]));
  }
  return send(404, '{"error":"not found"}');
});
server.on('upgrade', (req, socket) => {
  const path = (req.url || '').split('?')[0];
  const origin = (req.headers.origin || 'null').replace(/\/$/, '');
  const key = req.headers['sec-websocket-key'];
  if (!key || (path !== '/bridge' && !(STDIO && path === '/'))) { socket.end('HTTP/1.1 404 Not Found\r\n\r\n'); return; }
  if (ALLOWED.length && !ALLOWED.includes(origin)) { log('Puente: origen rechazado', origin); socket.end('HTTP/1.1 403 Forbidden\r\n\r\n'); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write(['HTTP/1.1 101 Switching Protocols', 'Upgrade: websocket', 'Connection: Upgrade', `Sec-WebSocket-Accept: ${accept}`, '', ''].join('\r\n'));
  socket.setNoDelay(true); socket.setKeepAlive(true, 30000);
  attachSocket(socket);
});
server.on('error', e => log('Error del servidor:', e.code === 'EADDRINUSE' ? `el puerto ${PORT} ya está en uso` : e.message));
server.listen(PORT, HOST, () => log(STDIO ? `Modo local: MCP por stdio · puente ws://127.0.0.1:${PORT}/bridge` : `Escuchando en el puerto ${PORT}. MCP: /mcp/<código> · Puente: /bridge`));

/* ============================================================ MCP por stdio (modo local) */
if (STDIO) {
  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', async line => {
    line = line.trim(); if (!line) return;
    let msg; try { msg = JSON.parse(line); } catch { process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'JSON inválido' } }) + '\n'); return; }
    const out = (await Promise.all((Array.isArray(msg) ? msg : [msg]).map(rpc))).filter(Boolean);
    for (const o of out) process.stdout.write(JSON.stringify(o) + '\n');
  });
  rl.on('close', () => { log('stdin cerrado, saliendo'); process.exit(0); });
}
process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));
