// --- Dependencies ---
const express = require("express");
const WebSocket = require("ws");
const { Pool } = require("pg");
const path = require("path");
const bodyParser = require("body-parser");
const http = require("http");
const moment = require("moment-timezone");

// --- Config ---
const PORT = process.env.PORT || 5000;
const TZ = "Asia/Kolkata";
const RETENTION_DAYS = 5;

// --- App & Server Setup ---
const app = express();
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// --- Database Setup ---
const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("localhost") ? false : { rejectUnauthorized: false },
});

async function initDB() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS stocks (
      stock TEXT PRIMARY KEY,
      trigger_price REAL,
      datetime TIMESTAMPTZ,
      count INTEGER,
      lastUpdated TIMESTAMPTZ,
      scan_name TEXT,
      scan_url TEXT,
      alert_name TEXT
    )
  `);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_datetime_stock ON stocks (datetime, stock)`);
  await db.query(`SET TIME ZONE 'UTC'`);
}
initDB().catch(console.error);

// --- Time Utilities ---
const timeUtils = {
  dayBoundsUTC: () => {
    const startIST = moment().tz(TZ).startOf("day");
    const endIST = moment().tz(TZ).endOf("day");
    return { startUTC: startIST.clone().utc().toDate(), endUTC: endIST.clone().utc().toDate() };
  },
  nowUTC: () => new Date(),
  // parse incoming "h:mm AM/PM" as *today in IST*, then convert to UTC
  parseISTTimeToUTC: (timeStr) => {
    const todayIST = moment().tz(TZ).format("YYYY-MM-DD");
    return moment.tz(`${todayIST} ${timeStr}`, "YYYY-MM-DD h:mm A", TZ).utc().toDate();
  },
};

// --- Fields projected in IST for the client ---
const SELECT_FIELDS_IST = `
  stock, trigger_price, count, scan_name, scan_url, alert_name,
  datetime AT TIME ZONE '${TZ}' AS datetime_ist,
  to_char(datetime AT TIME ZONE '${TZ}', 'YYYY-MM-DD') AS date_ist,
  to_char(datetime AT TIME ZONE '${TZ}', 'HH12:MI AM') AS time_ist,
  EXTRACT(EPOCH FROM (datetime AT TIME ZONE '${TZ}'))*1000 AS ts_ist,
  lastUpdated AT TIME ZONE '${TZ}' AS lastUpdated_ist
`;

// --- DB Utilities ---
const dbUtils = {
  getRangeIST: async (fromUTC, toUTC) => {
    const { rows } = await db.query(
      `SELECT ${SELECT_FIELDS_IST}
       FROM stocks
       WHERE datetime >= $1 AND datetime < $2
       ORDER BY datetime DESC`,
      [fromUTC, toUTC]
    );
    return rows;
  },

  upsertBatch: async (items) => {
    if (!items.length) return;
    const values = [];
    const placeholders = [];

    items.forEach((it, i) => {
      const base = i * 8;
      placeholders.push(
        `($${base + 1}, $${base + 2}, $${base + 3}, 1, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7})`
      );
      values.push(
        it.stock,
        it.trigger_price,
        it.datetimeUTC,
        it.lastUpdatedUTC,
        it.scan_name || null,
        it.scan_url || null,
        it.alert_name || null
      );
    });

    const sql = `
      INSERT INTO stocks (stock, trigger_price, datetime, count, lastUpdated, scan_name, scan_url, alert_name)
      VALUES ${placeholders.join(",")}
      ON CONFLICT (stock) DO UPDATE
      SET trigger_price = EXCLUDED.trigger_price,
          count = stocks.count + 1,
          datetime = EXCLUDED.datetime,
          lastUpdated = EXCLUDED.lastUpdated,
          scan_name = EXCLUDED.scan_name,
          scan_url = EXCLUDED.scan_url,
          alert_name = EXCLUDED.alert_name
    `;
    await db.query(sql, values);
  },

  clearLiveUTC: (fromUTC, toUTC) =>
    db.query(`DELETE FROM stocks WHERE datetime >= $1 AND datetime < $2`, [fromUTC, toUTC]),

  clearBeforeUTC: (beforeUTC) =>
    db.query(`DELETE FROM stocks WHERE datetime < $1`, [beforeUTC]),
};

// --- In-memory cache ---
let cache = { live: [], history: [] };

async function refreshCacheAndBroadcast(full = false, deltaLive = []) {
  const { startUTC, endUTC } = timeUtils.dayBoundsUTC();
  const fiveDaysAgoUTC = moment(startUTC).subtract(RETENTION_DAYS, "days").toDate();

  const [live, history] = await Promise.all([
    dbUtils.getRangeIST(startUTC, endUTC),
    dbUtils.getRangeIST(fiveDaysAgoUTC, startUTC),
  ]);

  cache.live = live;
  cache.history = history;

  if (full) {
    const payload = JSON.stringify({ type: "init", liveStocks: live, historyStocks: history });
    wss.clients.forEach((c) => c.readyState === WebSocket.OPEN && c.send(payload));
  } else if (deltaLive?.length) {
    const payload = JSON.stringify({ type: "delta", liveDelta: deltaLive });
    wss.clients.forEach((c) => c.readyState === WebSocket.OPEN && c.send(payload));
  }
}

// --- Request transform (from your sender/n8n) ---
function transformRequest(body) {
  const { stocks, trigger_prices, triggered_at, scan_name, scan_url, alert_name } = body;
  if (!stocks || !trigger_prices) return [];

  const stockArr = String(stocks).split(",").map((s) => s.trim()).filter(Boolean);
  const priceArr = String(trigger_prices).split(",").map((p) => parseFloat(String(p).trim()));
  const nowUTC = timeUtils.nowUTC();

  return stockArr
    .map((s, i) => ({
      stock: s,
      trigger_price: Number.isFinite(priceArr[i]) ? priceArr[i] : null,
      datetimeUTC: triggered_at ? timeUtils.parseISTTimeToUTC(triggered_at) : nowUTC,
      lastUpdatedUTC: nowUTC,
      scan_name,
      scan_url,
      alert_name,
    }))
    .filter((x) => x.stock && x.trigger_price !== null);
}

// --- WebSocket connection ---
wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  // push current snapshot immediately
  ws.send(JSON.stringify({ type: "init", liveStocks: cache.live, historyStocks: cache.history }));
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30_000);

// --- API Routes ---
app.post("/new-stocks", async (req, res) => {
  try {
    const items = transformRequest(req.body);
    if (!items.length) return res.json({ status: "ok" });

    await dbUtils.upsertBatch(items);

    // fetch only affected rows (formatted in IST for client)
    const stocks = items.map((i) => i.stock);
    const placeholders = stocks.map((_, idx) => `$${idx + 1}`).join(",");
    const { rows: deltaRows } = await db.query(
      `SELECT ${SELECT_FIELDS_IST} FROM stocks WHERE stock IN (${placeholders})`,
      stocks
    );

    await refreshCacheAndBroadcast(false, deltaRows);

    res.json({ status: "ok" });
  } catch (e) {
    console.error("POST /new-stocks error:", e);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

app.post("/clear-live", async (req, res) => {
  try {
    const { startUTC, endUTC } = timeUtils.dayBoundsUTC();
    await dbUtils.clearLiveUTC(startUTC, endUTC);
    await refreshCacheAndBroadcast(true);
    res.json({ status: "ok" });
  } catch (e) {
    console.error("POST /clear-live error:", e);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

app.post("/clear-history", async (req, res) => {
  try {
    const { startUTC } = timeUtils.dayBoundsUTC();
    await dbUtils.clearBeforeUTC(startUTC);
    await refreshCacheAndBroadcast(true);
    res.json({ status: "ok" });
  } catch (e) {
    console.error("POST /clear-history error:", e);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// --- Cleanup old records hourly ---
setInterval(async () => {
  try {
    const { startUTC } = timeUtils.dayBoundsUTC();
    const cutoffUTC = moment(startUTC).subtract(RETENTION_DAYS, "days").toDate();
    await dbUtils.clearBeforeUTC(cutoffUTC);
  } catch (e) {
    console.error("Cleanup error:", e);
  }
}, 60 * 60 * 1000);

// --- Prime cache on boot ---
(async () => {
  try {
    await refreshCacheAndBroadcast(true);
  } catch (e) {
    console.error("Initial cache error:", e);
  }
})();

// --- Start Server ---
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
