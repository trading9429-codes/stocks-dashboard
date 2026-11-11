// ============================================================
// server.js  —  Stocks Dashboard backend
// ============================================================

// --- Dependencies ---
const express  = require("express");
const WebSocket = require("ws");
const { Pool } = require("pg");
const path     = require("path");
const http     = require("http");
const https    = require("https");
const moment   = require("moment-timezone");

// --- Config ---
const PORT           = process.env.PORT           || 5000;
const TZ             = "Asia/Kolkata";
const RETENTION_DAYS = 5;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET; // optional — set in Render env vars
const SELF_URL       = process.env.RENDER_EXTERNAL_URL || process.env.APP_URL; // e.g. https://your-app.onrender.com

// --- App & Server ---
const app    = express();
app.use(express.json());                                      // built-in; body-parser not needed
app.use(express.static(path.join(__dirname, "public")));
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

// --- Database ---
const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("localhost")
        ? false
        : { rejectUnauthorized: false },
});

async function initDB() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS stocks (
      stock        TEXT PRIMARY KEY,
      trigger_price REAL,
      datetime      TIMESTAMPTZ,
      count         INTEGER,
      lastUpdated   TIMESTAMPTZ,
      scan_name     TEXT,
      scan_url      TEXT,
      alert_name    TEXT
    )
  `);
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_datetime_stock ON stocks (datetime, stock)`
  );
  console.log("[db] schema ready");
}

// --- Time helpers ---
const timeUtils = {
  dayBoundsUTC: () => {
    const startIST = moment().tz(TZ).startOf("day");
    const endIST   = moment().tz(TZ).endOf("day");
    return {
      startUTC: startIST.clone().utc().toDate(),
      endUTC:   endIST.clone().utc().toDate(),
    };
  },
  nowUTC: () => new Date(),
  parseISTTimeToUTC: (timeStr) => {
    const todayIST = moment().tz(TZ).format("YYYY-MM-DD");
    return moment
      .tz(`${todayIST} ${timeStr}`, "YYYY-MM-DD h:mm A", TZ)
      .utc()
      .toDate();
  },
  // Returns true if current IST time is within market + keep-alive window
  // Keep-alive window: 9:10 AM – 3:05 PM IST, Mon–Fri
  isKeepAliveWindow: () => {
    const now = moment().tz(TZ);
    const day = now.day(); // 0=Sun … 6=Sat
    if (day === 0 || day === 6) return false;
    const mins = now.hours() * 60 + now.minutes();
    return mins >= 9 * 60 + 10 && mins <= 15 * 60 + 5;
  },
};

// --- IST projection SQL fragment ---
const SELECT_IST = `
  stock, trigger_price, count, scan_name, scan_url, alert_name,
  to_char(datetime AT TIME ZONE '${TZ}', 'YYYY-MM-DD')   AS date_ist,
  to_char(datetime AT TIME ZONE '${TZ}', 'HH12:MI AM')   AS time_ist,
  EXTRACT(EPOCH FROM (datetime AT TIME ZONE '${TZ}'))*1000 AS ts_ist,
  to_char(lastUpdated AT TIME ZONE '${TZ}', 'HH12:MI AM') AS lastUpdated_ist
`;

// --- DB helpers ---
const dbUtils = {
  getRangeIST: async (fromUTC, toUTC) => {
    const { rows } = await db.query(
      `SELECT ${SELECT_IST}
         FROM stocks
        WHERE datetime >= $1 AND datetime < $2
        ORDER BY datetime DESC`,
      [fromUTC, toUTC]
    );
    return rows;
  },

  // Atomic upsert + IST projection in one round-trip using a CTE
  upsertAndFetchIST: async (items) => {
    if (!items.length) return [];

    const vals  = [];
    const parts = [];
    items.forEach((it, i) => {
      const b = i * 8;
      parts.push(
        `($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},$${b+7},$${b+8})`
      );
      vals.push(
        it.stock, it.trigger_price, it.datetimeUTC,
        1, it.lastUpdatedUTC,
        it.scan_name || null, it.scan_url || null, it.alert_name || null
      );
    });

    const { rows } = await db.query(
      `WITH upserted AS (
         INSERT INTO stocks
           (stock, trigger_price, datetime, count, lastUpdated, scan_name, scan_url, alert_name)
         VALUES ${parts.join(",")}
         ON CONFLICT (stock) DO UPDATE SET
           trigger_price = EXCLUDED.trigger_price,
           count         = stocks.count + 1,
           datetime      = EXCLUDED.datetime,
           lastUpdated   = EXCLUDED.lastUpdated,
           scan_name     = EXCLUDED.scan_name,
           scan_url      = EXCLUDED.scan_url,
           alert_name    = EXCLUDED.alert_name
         RETURNING *
       )
       SELECT ${SELECT_IST} FROM upserted`,
      vals
    );
    return rows;
  },

  clearLiveUTC: (fromUTC, toUTC) =>
    db.query(`DELETE FROM stocks WHERE datetime >= $1 AND datetime < $2`, [fromUTC, toUTC]),

  clearBeforeUTC: (beforeUTC) =>
    db.query(`DELETE FROM stocks WHERE datetime < $1`, [beforeUTC]),
};

// --- In-memory cache ---
let cache = { live: [], history: [] };

async function refreshCache(full = false, deltaLive = []) {
  const { startUTC, endUTC }  = timeUtils.dayBoundsUTC();
  const fiveDaysAgoUTC = moment(startUTC).subtract(RETENTION_DAYS, "days").toDate();

  const [live, history] = await Promise.all([
    dbUtils.getRangeIST(startUTC, endUTC),
    dbUtils.getRangeIST(fiveDaysAgoUTC, startUTC),
  ]);
  cache.live    = live;
  cache.history = history;

  const broadcast = (payload) =>
    wss.clients.forEach((c) => c.readyState === WebSocket.OPEN && c.send(payload));

  if (full) {
    broadcast(JSON.stringify({ type: "init", liveStocks: live, historyStocks: history }));
  } else if (deltaLive.length) {
    broadcast(JSON.stringify({ type: "delta", liveDelta: deltaLive }));
  }
}

// --- Webhook auth middleware ---
function verifySecret(req, res, next) {
  if (!WEBHOOK_SECRET) return next();           // not configured → open
  const token = req.headers["x-webhook-secret"] || req.body?.secret;
  if (token !== WEBHOOK_SECRET)
    return res.status(401).json({ error: "Unauthorized" });
  next();
}

// --- Transform Chartink webhook payload ---
function transform(body) {
  const { stocks, trigger_prices, triggered_at, scan_name, scan_url, alert_name } = body;
  if (!stocks || !trigger_prices) return [];

  const stockArr = String(stocks).split(",").map((s) => s.trim()).filter(Boolean);
  const priceArr = String(trigger_prices).split(",").map((p) => parseFloat(p.trim()));
  const nowUTC   = timeUtils.nowUTC();

  return stockArr
    .map((s, i) => ({
      stock:         s,
      trigger_price: Number.isFinite(priceArr[i]) ? priceArr[i] : null,
      datetimeUTC:   triggered_at ? timeUtils.parseISTTimeToUTC(triggered_at) : nowUTC,
      lastUpdatedUTC: nowUTC,
      scan_name, scan_url, alert_name,
    }))
    .filter((x) => x.stock && x.trigger_price !== null);
}

// --- WebSocket heartbeat ---
wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));
  // Send current snapshot immediately on connect
  ws.send(JSON.stringify({
    type: "init",
    liveStocks: cache.live,
    historyStocks: cache.history,
  }));
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30_000);

// ============================================================
// Routes
// ============================================================

// Health check (also used by keep-alive pings)
app.get("/health", (req, res) =>
  res.json({ status: "ok", uptime: Math.floor(process.uptime()), clients: wss.clients.size })
);

// Chartink webhook
app.post("/new-stocks", verifySecret, async (req, res) => {
  try {
    const items = transform(req.body);
    if (!items.length) return res.json({ status: "ok", processed: 0 });

    const deltaRows = await dbUtils.upsertAndFetchIST(items);
    await refreshCache(false, deltaRows);

    res.json({ status: "ok", processed: items.length });
  } catch (e) {
    console.error("[/new-stocks]", e.message);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Clear today's live data
app.post("/clear-live", async (req, res) => {
  try {
    const { startUTC, endUTC } = timeUtils.dayBoundsUTC();
    await dbUtils.clearLiveUTC(startUTC, endUTC);
    await refreshCache(true);
    res.json({ status: "ok" });
  } catch (e) {
    console.error("[/clear-live]", e.message);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Clear historical data (before today)
app.post("/clear-history", async (req, res) => {
  try {
    const { startUTC } = timeUtils.dayBoundsUTC();
    await dbUtils.clearBeforeUTC(startUTC);
    await refreshCache(true);
    res.json({ status: "ok" });
  } catch (e) {
    console.error("[/clear-history]", e.message);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// ============================================================
// Scheduled jobs
// ============================================================

// Hourly retention cleanup
setInterval(async () => {
  try {
    const { startUTC } = timeUtils.dayBoundsUTC();
    const cutoff = moment(startUTC).subtract(RETENTION_DAYS, "days").toDate();
    await dbUtils.clearBeforeUTC(cutoff);
    console.log("[cleanup] old records purged");
  } catch (e) {
    console.error("[cleanup]", e.message);
  }
}, 60 * 60 * 1000);

// Self-ping every 4 min during market keep-alive window (9:10–15:05 IST, Mon–Fri)
// Prevents Render free-tier from sleeping during market hours.
// Set RENDER_EXTERNAL_URL or APP_URL env var to your deployed URL.
if (SELF_URL) {
  console.log(`[keep-alive] enabled — will ping ${SELF_URL}/health during market hours`);
  setInterval(() => {
    if (!timeUtils.isKeepAliveWindow()) return;
    try {
      const target = new URL("/health", SELF_URL);
      const lib    = target.protocol === "https:" ? https : http;
      lib.get(target.href)
         .on("response", (r) => console.log(`[keep-alive] ping → ${r.statusCode}`))
         .on("error",    (e) => console.warn(`[keep-alive] ${e.message}`));
    } catch (e) {
      console.warn("[keep-alive] bad URL:", e.message);
    }
  }, 4 * 60 * 1000);
} else {
  console.log("[keep-alive] RENDER_EXTERNAL_URL / APP_URL not set — keep-alive disabled");
}

// ============================================================
// Boot
// ============================================================
async function main() {
  try {
    await initDB();
    await refreshCache(true);                 // prime cache before accepting requests
    server.listen(PORT, () =>
      console.log(`[server] listening on port ${PORT}`)
    );
  } catch (e) {
    console.error("[startup] fatal:", e);
    process.exit(1);
  }
}

main();
