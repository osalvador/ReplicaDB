const CACHE_NAME = 'markup-forge-ide-v89';
const ASSETS = [
  './converter.html',
  './converter-core.js',
  './assets/app.js',
  './manifest.webmanifest',
  './markdown-copy-lab-icon.svg',
  './markdown-copy-lab-icon-192.png',
  './markdown-copy-lab-icon-512.png',
  './apple-touch-icon.png',
  './favicon-32.png'
];

self.addEventListener('install', event => {
  // Pre-cache with { cache: 'reload' } so we never store a stale HTTP-cached copy.
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache =>
      cache.addAll(ASSETS.map(url => new Request(url, { cache: 'reload' })))
    )
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(caches.keys().then(keys => Promise.all(keys.filter(key => key !== CACHE_NAME).map(key => caches.delete(key)))));
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  const req = event.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  // Network-first for code/markup so the latest version is always used when
  // online; fall back to the cache when offline. Other assets (icons, fonts)
  // stay cache-first for speed.
  const isCode = req.mode === 'navigate'
    || /\.(?:html|js|css|webmanifest)$/.test(url.pathname);
  if (isCode) {
    event.respondWith(
      fetch(req)
        .then(res => {
          const copy = res.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(req, copy));
          return res;
        })
        .catch(() => caches.match(req))
    );
    return;
  }
  event.respondWith(caches.match(req).then(cached => cached || fetch(req)));
});
