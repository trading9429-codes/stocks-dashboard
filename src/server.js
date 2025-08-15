const express = require("express");
const WebSocket = require("ws");
const { Pool } = require("pg");

const path = require("path");
const bodyParser = require("body-parser");
const http = require("http");
const moment = require("moment-timezone");

const app = express();
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));

// --- Database setup ---

    
const db = new Pool({ connectionString: process.env.DATABASE_URL,
   ssl: { rejectUnauthorized: false }  });// Render Postgres requires SSL
// Create table and indexes
(async () => {
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
})();

// --- IST Helpers ---
function getISTDate() {
    return moment().tz("Asia/Kolkata").toDate();
}

function getISTDayBounds() {
    const startIST = moment().tz("Asia/Kolkata").startOf("day").toDate();
    const endIST = moment().tz("Asia/Kolkata").endOf("day").toDate();
    return { startIST, endIST };
}

function convertToTodayLocal(timeStr) {
    // timeStr format example: "3:45 PM"
    return moment.tz(timeStr, "h:mm A", "Asia/Kolkata").toDate();
}

// --- HTTP + WebSocket server ---
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// --- Broadcast function ---
async function broadcastStocks() {
    const { startIST: todayStart, endIST: tomorrowStart } = getISTDayBounds();
    // Live stocks: today
    try {
        // Live stocks (today only)
        const liveStocksRes = await db.query(
            "SELECT * FROM stocks WHERE datetime >= $1 AND datetime < $2 ORDER BY datetime ASC",
            [todayStart, tomorrowStart]
        );
        const liveStocks = liveStocksRes.rows;

        // History stocks (last 5 days excluding today)
        const fiveDaysAgo = moment(todayStart).subtract(5, "days").toDate();
        const historyStocksRes = await db.query(
            "SELECT * FROM stocks WHERE datetime >= $1 AND datetime < $2 ORDER BY datetime DESC",
            [fiveDaysAgo, todayStart]
        );
        const historyStocks = historyStocksRes.rows;

        const payload = { liveStocks, historyStocks };
        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify(payload));
            }
        });
    } catch (err) {
        console.error(err);
    }
}

// --- WebSocket connection ---
wss.on("connection", ws => {
    console.log("Client connected");
    broadcastStocks();
});

// --- Transform request helper ---
function transformRequest(res_data) {
    const stocksArr = res_data.body.stocks.split(",");
    const pricesArr = res_data.body.trigger_prices.split(",");
    const triggeredAt = res_data.body.triggered_at;
    const scanName = res_data.body.scan_name;
    const scanUrl = res_data.body.scan_url;
    const alertName = res_data.body.alert_name;

    const output = [];
    for (let i = 0; i < stocksArr.length; i++) {
        output.push({
            stock: stocksArr[i].trim(),
            trigger_price: parseFloat(pricesArr[i].trim()),
            triggered_at: triggeredAt,
            scan_name: scanName,
            scan_url: scanUrl,
            alert_name: alertName
        });
    }   

return { data: output };
}

// --- New stocks endpoint ---
app.post("/new-stocks", async (req, res) => {
    const op_data = transformRequest(req);
    const stocks = op_data?.data || [];
    if (stocks.length === 0) return res.send({ status: "ok" });

    const lastUpdated = getISTDate();
    let completed = 0;

    try {
        for (const item of stocks) {
            const datetime = item.triggered_at
                ? convertToTodayLocal(item.triggered_at)
                : lastUpdated;

            await db.query(`
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
            `, [
                item.stock,
                item.trigger_price,
                datetime,
                lastUpdated,
                item.scan_name,
                item.scan_url,
                item.alert_name
            ]);
        }
        broadcastStocks();
    } catch (err) {
        console.error(err);
    }
    res.send({ status: "ok" });
});

// --- Clear live stocks ---
app.post("/clear-live", async (req, res) => {
    const { startIST: todayStart, endIST: tomorrowStart } = getISTDayBounds();
    await db.query("DELETE FROM stocks WHERE datetime >= $1 AND datetime < $2", [todayStart, tomorrowStart]);
    broadcastStocks();
    res.send({ status: "ok" });
});

// --- Clear history stocks ---
app.post("/clear-history", async (req, res) => {
    const { startIST: todayStart } = getISTDayBounds();
    await db.query("DELETE FROM stocks WHERE datetime < $1", [todayStart]);
    broadcastStocks();
    res.send({ status: "ok" });
});

// --- Cleanup older than 5 days ---
setInterval(async () => {
    const cutoffDate = moment().tz("Asia/Kolkata").subtract(5, "days").toDate();
    await db.query("DELETE FROM stocks WHERE datetime < $1", [cutoffDate]);
    broadcastStocks();
}, 1000 * 60 * 60); // every hour


// --- Start server ---
const PORT = process.env.PORT || 5000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
