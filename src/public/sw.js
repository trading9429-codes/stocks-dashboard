// ============================================================
// Service Worker — Desktop & Mobile Notifications handler
// ============================================================

self.addEventListener("install", (event) => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

// Handle clicks on system notifications
self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl = event.notification.data?.url || "/";

  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientList) => {
      // If a tab is already open, focus it
      for (const client of clientList) {
        if ("focus" in client) {
          if (event.notification.data?.url && event.notification.data.url.startsWith("http")) {
            // Open chart in new tab if specific chart requested
            return clients.openWindow(targetUrl);
          }
          return client.focus();
        }
      }
      // If no open tabs, open a new window
      if (clients.openWindow) {
        return clients.openWindow(targetUrl);
      }
    })
  );
});
