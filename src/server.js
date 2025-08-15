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

// --- App & Server Setup ---
const app = express();
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// --- Database Setup ---

const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function initDB() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS stocks (
      stock TEXT PRIMARY KEY,
      trigger_price REAL,
      datetime TIMESTAMP WITHOUT TIME ZONE,
      count INTEGER,
      lastUpdated TIMESTAMP WITHOUT TIME ZONE,
      scan_name TEXT,
      scan_url TEXT,
      alert_name TEXT
    )
  `);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_datetime ON stocks(datetime)`);
}
initDB().catch(console.error);

// --- Time Utilities ---
const timeUtils = {
  now: () => moment().tz(TZ).toDate(),
  dayBounds: () => ({
    start: moment().tz(TZ).startOf("day").toDate(),
    end: moment().tz(TZ).endOf("day").toDate()
  }),
  parseTodayTime: (timeStr) => moment.tz(timeStr, "h:mm A", TZ).toDate()
};

// --- DB Utilities ---
const dbUtils = {
  getLiveStocks: (start, end) =>
    db.query(`SELECT * FROM stocks WHERE datetime >= $1 AND datetime < $2 ORDER BY datetime ASC`, [start, end]),
  getHistoryStocks: (from, to) =>
    db.query(`SELECT * FROM stocks WHERE datetime >= $1 AND datetime < $2 ORDER BY datetime DESC`, [from, to]),
  insertOrUpdateStock: (item, datetime, lastUpdated) =>
    db.query(
      `
      INSERT INTO stocks (stock, trigger_price, datetime, count, lastUpdated, scan_name, scan_url, alert_name)
      VALUES ($1, $2, $3, 1, $4, $5, $6, $7)
      ON CONFLICT (stock) DO UPDATE
      SET trigger_price = EXCLUDED.trigger_price,
          count = stocks.count + 1,
          datetime = EXCLUDED.datetime,
          lastUpdated = EXCLUDED.lastUpdated,
          scan_name = EXCLUDED.scan_name,
          scan_url = EXCLUDED.scan_url,
          alert_name = EXCLUDED.alert_name
      `,
      [
        item.stock,
        item.trigger_price,
        datetime,
        lastUpdated,
        item.scan_name,
        item.scan_url,
        item.alert_name
      ]
    ),
  clearLive: (start, end) =>
    db.query(`DELETE FROM stocks WHERE datetime >= $1 AND datetime < $2`, [start, end]),
  clearHistory: (before) =>
    db.query(`DELETE FROM stocks WHERE datetime < $1`, [before]),
  cleanupOld: (before) =>
    db.query(`DELETE FROM stocks WHERE datetime < $1`, [before])
};

// --- WebSocket Utilities ---
async function broadcastStocks() {
  const { start, end } = timeUtils.dayBounds();
  const fiveDaysAgo = moment(start).subtract(5, "days").toDate();

  try {
    const [live, history] = await Promise.all([
      dbUtils.getLiveStocks(start, end),
      dbUtils.getHistoryStocks(fiveDaysAgo, start)
    ]);

    const payload = {
      liveStocks: live.rows,
      historyStocks: history.rows
    };

    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify(payload));
      }
    });
  } catch (err) {
    console.error("Broadcast error:", err);
  }
}

// --- Request Parsing ---
function transformRequest(req) {
  const { stocks, trigger_prices, triggered_at, scan_name, scan_url, alert_name } = req.body;
  if (!stocks || !trigger_prices) return [];

  const stockArr = stocks.split(",").map(s => s.trim());
  const priceArr = trigger_prices.split(",").map(p => parseFloat(p.trim()));

  return stockArr.map((stock, i) => ({
    stock,
    trigger_price: priceArr[i],
    triggered_at,
    scan_name,
    scan_url,
    alert_name
  }));
}

// --- Async Route Wrapper ---
const asyncHandler = fn => (req, res) => fn(req, res).catch(err => {
  console.error(err);
  res.status(500).send({ error: "Internal Server Error" });
});

// --- API Routes ---
app.post("/new-stocks", asyncHandler(async (req, res) => {
  const stocks = transformRequest(req);
  if (!stocks.length) return res.send({ status: "ok" });

  const lastUpdated = timeUtils.now();

  await Promise.all(stocks.map(item => {
    const datetime = item.triggered_at
      ? timeUtils.parseTodayTime(item.triggered_at)
      : lastUpdated;
    return dbUtils.insertOrUpdateStock(item, datetime, lastUpdated);
  }));

  await broadcastStocks();
  res.send({ status: "ok" });
}));

app.post("/clear-live", asyncHandler(async (req, res) => {
  const { start, end } = timeUtils.dayBounds();
  await dbUtils.clearLive(start, end);
  await broadcastStocks();
  res.send({ status: "ok" });
}));

app.post("/clear-history", asyncHandler(async (req, res) => {
  const { start } = timeUtils.dayBounds();
  await dbUtils.clearHistory(start);
  await broadcastStocks();
  res.send({ status: "ok" });
}));

// --- Cleanup old records every hour ---
setInterval(async () => {
  const cutoffDate = moment().tz(TZ).subtract(5, "days").toDate();
  await dbUtils.cleanupOld(cutoffDate);
  await broadcastStocks();
}, 60 * 60 * 1000);

// --- WebSocket Events ---
wss.on("connection", () => {
  console.log("Client connected");
  broadcastStocks();
});

// --- Start Server ---
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
