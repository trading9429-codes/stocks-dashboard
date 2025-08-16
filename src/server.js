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
// const db = new Pool({
//   connectionString: process.env.DATABASE_URL,
//   ssl: { rejectUnauthorized: false }
// });


const db = new Pool({
    connectionString: "postgresql://hosted_postgress_db_user:NRxATsyG8KWI26XdrKX3CMJm2CEvFELg@dpg-d2fjh5ggjchc73fo8n50-a.oregon-postgres.render.com/hosted_postgress_db",
    ssl: { rejectUnauthorized: false }
});

async function initDB() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS stocks (
      stock TEXT PRIMARY KEY,
      trigger_price REAL,
      datetime TIMESTAMPTZ,          -- store as timestamptz (UTC internally)
      count INTEGER,
      lastUpdated TIMESTAMPTZ,
      scan_name TEXT,
      scan_url TEXT,
      alert_name TEXT
    )
  `);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_datetime_stock ON stocks (datetime, stock)`);
}
initDB().catch(console.error);

// --- Time Utilities ---
const timeUtils = {
  // IST day bounds converted to UTC instant for DB filtering
  dayBoundsUTC: () => {
    const startIST = moment().tz(TZ).startOf("day");
    const endIST = moment().tz(TZ).endOf("day");
    return { startUTC: startIST.clone().utc().toDate(), endUTC: endIST.clone().utc().toDate() };
  },
  nowUTC: () => new Date(), // NOW in UTC
  // parse an "h:mm AM/PM" time as *today in IST*, then convert to UTC Date
  parseISTTimeToUTC: (timeStr) => {
  // Combine today's date in IST with the given time string
  const todayIST = moment().tz(TZ).format("YYYY-MM-DD");
  let a =moment.tz(`${todayIST} ${timeStr}`, "YYYY-MM-DD h:mm A", TZ).toDate();
  return a;
},
};

// --- DB Utilities ---
const SELECT_FIELDS_IST = `
  stock, trigger_price, count, scan_name, scan_url, alert_name,
  to_char(datetime AT TIME ZONE '${TZ}', 'YYYY-MM-DD"T"HH24:MI:SS') AS datetime
`;

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

// --- In-memory cache for quick init pushes ---
let cache = {
  live: [],
  history: []
};

async function refreshCacheAndBroadcast(full = false, deltaLive = []) {
  const { startUTC, endUTC } = timeUtils.dayBoundsUTC();
  const fiveDaysAgoUTC = moment(startUTC).subtract(RETENTION_DAYS, "days").toDate();

  const [live, history] = await Promise.all([
    dbUtils.getRangeIST(startUTC, endUTC),
    dbUtils.getRangeIST(fiveDaysAgoUTC, startUTC)
  ]);

  cache.live = live;
  cache.history = history;

  if (full) {
    // full snapshot to everyone
    const data = JSON.stringify({ type: "init", liveStocks: live, historyStocks: history });
    wss.clients.forEach(c => c.readyState === WebSocket.OPEN && c.send(data));
  } else if (deltaLive && deltaLive.length) {
    // only changed/added rows
    const data = JSON.stringify({ type: "delta", liveDelta: deltaLive });
    wss.clients.forEach(c => c.readyState === WebSocket.OPEN && c.send(data));
  }
}

// --- Request Parsing (compatible with your current sender) ---
function transformRequest(body) {
  const { stocks, trigger_prices, triggered_at, scan_name, scan_url, alert_name } = body;
  if (!stocks || !trigger_prices) return [];

  const stockArr = String(stocks).split(",").map(s => s.trim()).filter(Boolean);
  const priceArr = String(trigger_prices).split(",").map(p => parseFloat(String(p).trim()));
  const nowUTC = timeUtils.nowUTC();

  return stockArr.map((s, i) => ({
    stock: s,
    trigger_price: Number.isFinite(priceArr[i]) ? priceArr[i] : null,
    datetimeUTC: triggered_at
      ? timeUtils.parseISTTimeToUTC(triggered_at)  // ✅ Always parse as today's IST time
      : nowUTC,
    lastUpdatedUTC: nowUTC,
    scan_name,
    scan_url,
    alert_name
  })).filter(x => x.stock && x.trigger_price !== null);
}

// --- WebSocket Heartbeat ---
wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  // send current snapshot immediately
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

    // Query only the affected rows back (in IST) to push as delta
    const stocks = items.map(i => i.stock);
    const placeholders = stocks.map((_, idx) => `$${idx + 1}`).join(",");
    const { rows: deltaRows } = await db.query(
      `SELECT ${SELECT_FIELDS_IST} FROM stocks WHERE stock IN (${placeholders})`,
      stocks
    );

    // Update caches and broadcast delta
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
    await refreshCacheAndBroadcast(true); // full refresh after clear
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
    await refreshCacheAndBroadcast(true); // full refresh after clear
    res.json({ status: "ok" });
  } catch (e) {
    console.error("POST /clear-history error:", e);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// --- Cleanup old records every hour ---
setInterval(async () => {
  try {
    const { startUTC } = timeUtils.dayBoundsUTC();
    const cutoffUTC = moment(startUTC).subtract(RETENTION_DAYS, "days").toDate();
    await dbUtils.clearBeforeUTC(cutoffUTC);
    // no broadcast needed here; a full refresh will happen on next change
  } catch (e) {
    console.error("Cleanup error:", e);
  }
}, 60 * 60 * 1000);

// --- Prime the cache on boot ---
(async () => {
  try {
    await refreshCacheAndBroadcast(true);
  } catch (e) {
    console.error("Initial cache error:", e);
  }
})();

// --- Start Server ---
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
