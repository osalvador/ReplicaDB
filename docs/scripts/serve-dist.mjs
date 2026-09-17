import { createReadStream, existsSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../dist/', import.meta.url));
const port = Number(process.env.DOCS_PORT ?? 4177);
const base = '/ReplicaDB';
/** @type {Record<string, string>} */
const contentTypes = {
  '.css': 'text/css; charset=utf-8', '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8', '.png': 'image/png', '.svg': 'image/svg+xml', '.txt': 'text/plain; charset=utf-8',
  '.webmanifest': 'application/manifest+json'
};

const server = createServer((request, response) => {
  const pathname = new URL(request.url ?? '/', `http://${request.headers.host}`).pathname;
  if (!pathname.startsWith(base)) { response.writeHead(404); response.end('Not found'); return; }
  const requested = pathname.slice(base.length) || '/';
  const safePath = normalize(requested).replace(/^\.\.(\/|\\)/, '');
  let filePath = join(root, safePath);
  if (filePath.endsWith('/')) filePath = join(filePath, 'index.html');
  if (!existsSync(filePath) || !statSync(filePath).isFile()) filePath = join(root, '404.html');
  response.writeHead(filePath.endsWith('404.html') ? 404 : 200, {
    'Content-Type': contentTypes[extname(filePath)] ?? 'application/octet-stream',
    'Cache-Control': 'no-store'
  });
  createReadStream(filePath).pipe(response);
});

server.listen(port, '127.0.0.1', () => process.stdout.write(`Docs preview available at http://127.0.0.1:${port}${base}/\n`));