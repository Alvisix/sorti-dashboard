// Service Worker minimale ma "installabile" per Chrome

self.addEventListener("install", () => {
  self.skipWaiting();
});

self.addEventListener("activate", () => {
  self.clients.claim();
});

// fetch handler (serve per soddisfare i criteri PWA di Chrome)
self.addEventListener("fetch", (event) => {
  event.respondWith(fetch(event.request));
});
