// ============================================================
// Service Worker — Mobile PWA & Push Notifications Engine
// ============================================================

const CACHE_NAME = "stock-dashboard-v5.0";
const STATIC_ASSETS = [
  "/manifest.json",
  "/res/icon.svg",
  "/res/new-notification-09-352705.mp3"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(STATIC_ASSETS).catch((err) => {
        console.warn("[sw] cache.addAll non-critical warning:", err);
      });
    })
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) => {
      return Promise.all(
        keys.map((k) => caches.delete(k))
      );
    }).then(() => self.clients.claim())
  );
});

// HTML & Live data: Always fresh from network; Media/audio: Cache-first
self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // Skip WebSocket, API, and webhook requests
  if (
    url.protocol === "ws:" ||
    url.protocol === "wss:" ||
    url.pathname.startsWith("/new-stocks") ||
    url.pathname.startsWith("/clear") ||
    url.pathname.startsWith("/subscribe") ||
    event.request.method !== "GET"
  ) {
    return;
  }

  // HTML / navigation requests: ALWAYS fetch fresh from network to prevent stale UI
  if (event.request.mode === "navigate" || url.pathname === "/" || url.pathname.endsWith(".html")) {
    event.respondWith(
      fetch(event.request).catch(() => caches.match(event.request))
    );
    return;
  }

  // Static media cache-first
  if (url.pathname.startsWith("/res/") || url.pathname.endsWith(".mp3") || url.pathname.endsWith(".svg")) {
    event.respondWith(
      caches.match(event.request).then((cached) => {
        return cached || fetch(event.request).then((resp) => {
          if (resp && resp.status === 200) {
            const clone = resp.clone();
            caches.open(CACHE_NAME).then((c) => c.put(event.request, clone));
          }
          return resp;
        });
      })
    );
    return;
  }

  // Network-first for everything else
  event.respondWith(
    fetch(event.request).catch(() => caches.match(event.request))
  );
});

// ============================================================
// Web Push Event Handler (Mobile Screen Off / App Background)
// ============================================================
self.addEventListener("push", (event) => {
  let data = {};
  if (event.data) {
    try {
      data = event.data.json();
    } catch (_) {
      data = { title: "Stock Alert", body: event.data.text() };
    }
  } else {
    data = { title: "Stock Alert", body: "New Chartink scan trigger" };
  }

  const stock = data.stock || "ALERT";
  const title = data.title || `🟢 ${stock} Alert`;
  const tvUrl = data.tvUrl || `https://in.tradingview.com/chart/?symbol=NSE:${encodeURIComponent(stock)}`;

  const isHighVol = Boolean(data.isHighVol);
  const vibPattern = isHighVol ? [300, 80, 300, 80, 300] : [150, 80, 150];

  const options = {
    body: data.body || `Triggered @ ₹${data.price || "—"} IST`,
    icon: "/res/icon.svg",
    badge: "/res/icon.svg",
    tag: `stock-${stock}`,
    renotify: true,
    vibrate: vibPattern,
    data: {
      url: tvUrl,
      stock: stock,
      price: data.price,
      scan: data.scan
    },
    actions: [
      { action: "chart", title: "📊 5m Chart" },
      { action: "sizer", title: "⚡ Sizer" },
      { action: "dismiss", title: "✕ Dismiss" }
    ]
  };

  event.waitUntil(self.registration.showNotification(title, options));
});

// Handle clicks on system and push notifications
self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const notifData = event.notification.data || {};
  const action = event.action;

  let targetUrl = notifData.url || "/";
  if (action === "chart") {
    targetUrl = `https://in.tradingview.com/chart/?symbol=NSE:${encodeURIComponent(notifData.stock || "")}`;
  } else if (action === "sizer") {
    targetUrl = `/?openSizer=${encodeURIComponent(notifData.stock || "")}&price=${encodeURIComponent(notifData.price || "")}`;
  } else if (action === "dismiss") {
    return;
  }

  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientList) => {
      if (action === "chart" && targetUrl.startsWith("http")) {
        return clients.openWindow ? clients.openWindow(targetUrl) : null;
      }
      for (const client of clientList) {
        if ("focus" in client) {
          if (action === "sizer") {
            client.postMessage({ type: "OPEN_SIZER", stock: notifData.stock, price: notifData.price });
          }
          return client.focus();
        }
      }
      if (clients.openWindow) {
        return clients.openWindow(targetUrl);
      }
    })
  );
});
