const CACHE_NAME = 'markup-forge-ide-v49';
const ASSETS = [
  './converter.html',
  './converter-core.js',
  './manifest.webmanifest',
  './markdown-copy-lab-icon.svg',
  './markdown-copy-lab-icon-192.png',
  './markdown-copy-lab-icon-512.png',
  './apple-touch-icon.png',
  './favicon-32.png'
];

self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(ASSETS)));
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(caches.keys().then(keys => Promise.all(keys.filter(key => key !== CACHE_NAME).map(key => caches.delete(key)))));
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  if (event.request.method !== 'GET') return;
  event.respondWith(caches.match(event.request).then(cached => cached || fetch(event.request)));
});
