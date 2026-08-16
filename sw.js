// SmartFX service worker - handles incoming push notifications and what
// happens when the person taps one. Kept deliberately small: it doesn't
// do offline caching, only push handling, since that's all this phase
// needs.

self.addEventListener("push", (event) => {
  let payload = {};
  try {
    payload = event.data ? event.data.json() : {};
  } catch (e) {
    payload = { title: "SmartFX", body: event.data ? event.data.text() : "" };
  }

  const title = payload.title || "SmartFX";
  const options = {
    body: payload.body || "",
    icon: "icon-192.png",
    badge: "icon-192.png",
    data: { url: payload.url || "dashboard.html" },
  };

  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl = event.notification.data?.url || "dashboard.html";

  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientList) => {
      // If the app is already open, focus it and navigate in place
      // rather than opening a second window.
      for (const client of clientList) {
        if ("focus" in client) {
          client.focus();
          client.navigate(targetUrl);
          return;
        }
      }
      return clients.openWindow(targetUrl);
    })
  );
});
