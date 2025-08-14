const express = require("express");
const WebSocket = require("ws");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const bodyParser = require("body-parser");
const http = require("http");

const app = express();
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));

// --- Database setup ---
const db = new sqlite3.Database("stocks.db");
db.run(`CREATE TABLE IF NOT EXISTS stocks (
    stock TEXT PRIMARY KEY,
    trigger_price REAL,
    datetime TEXT,
    count INTEGER,
    lastUpdated TEXT,
    scan_name TEXT,
    scan_url TEXT,
    alert_name TEXT
)`);

// --- Helpers ---
function getCurrentDateTime() {
    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = String(now.getMonth()+1).padStart(2,'0');
    const dd = String(now.getDate()).padStart(2,'0');
    const hh = String(now.getHours()).padStart(2,'0');
    const min = String(now.getMinutes()).padStart(2,'0');
    const sec = String(now.getSeconds()).padStart(2,'0');
    return `${yyyy}-${mm}-${dd} ${hh}:${min}:${sec}`;
}

// Convert "h:mm am/pm" to today’s local datetime string (IST)
function convertToTodayLocal(timeStr) {
    const [time, ampm] = timeStr.split(" ");
    let [hours, minutes] = time.split(":").map(Number);
    if(ampm.toLowerCase() === 'pm' && hours < 12) hours += 12;
    if(ampm.toLowerCase() === 'am' && hours === 12) hours = 0;

    const now = new Date();
    now.setHours(hours, minutes, 0, 0);

    const yyyy = now.getFullYear();
    const mm = String(now.getMonth() + 1).padStart(2,'0');
    const dd = String(now.getDate()).padStart(2,'0');
    const hh = String(now.getHours()).padStart(2,'0');
    const min = String(now.getMinutes()).padStart(2,'0');
    const sec = String(now.getSeconds()).padStart(2,'0');

    return `${yyyy}-${mm}-${dd} ${hh}:${min}:${sec}`;
}

// --- HTTP + WebSocket server ---
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// --- Broadcast function ---
function broadcastStocks() {
    const todayStr = new Date().toISOString().slice(0,10); // YYYY-MM-DD

    // Live stocks: today
    db.all(
        "SELECT * FROM stocks WHERE datetime LIKE ? ORDER BY datetime ASC",
        [`${todayStr}%`],
        (err, liveStocks) => {
            if(err) return console.error(err);

            // History stocks: last 5 days excluding today
            const fiveDaysAgo = new Date(Date.now() - 5*24*60*60*1000).toISOString().slice(0,10);
            db.all(
                "SELECT * FROM stocks WHERE datetime BETWEEN ? AND ? AND datetime NOT LIKE ? ORDER BY datetime DESC",
                [fiveDaysAgo, todayStr, `${todayStr}%`],
                (err2, historyStocks) => {
                    if(err2) return console.error(err2);

                    const payload = { liveStocks, historyStocks };

                    wss.clients.forEach(client => {
                        if(client.readyState === WebSocket.OPEN){
                            client.send(JSON.stringify(payload));
                        }
                    });
                }
            );
        }
    );
}

// --- WebSocket connection ---
wss.on("connection", ws => {
    console.log("Client connected");
    broadcastStocks();
});

// --- New stocks endpoint ---
app.post("/new-stocks", (req,res) => {
    const stocks = req.body[0]?.data || [];
    if(stocks.length === 0) return res.send({ status: "ok" });

    const lastUpdated = getCurrentDateTime();
    let completed = 0;

    stocks.forEach(item => {
        // Convert webhook time to ISO datetime
        const datetime = item.triggered_at ? convertToTodayLocal(item.triggered_at) : lastUpdated;

        const sql = `
            INSERT INTO stocks
                (stock, trigger_price, datetime, count, lastUpdated, scan_name, scan_url, alert_name)
            VALUES (?, ?, ?, 1, ?, ?, ?, ?)
            ON CONFLICT(stock) DO UPDATE SET
                trigger_price=excluded.trigger_price,
                count = stocks.count + 1,
                datetime=excluded.datetime,
                lastUpdated=excluded.lastUpdated,
                scan_name=excluded.scan_name,
                scan_url=excluded.scan_url,
                alert_name=excluded.alert_name
        `;

        db.run(sql, [
            item.stock,
            item.trigger_price,
            datetime,
            lastUpdated,
            item.scan_name,
            item.scan_url,
            item.alert_name
        ], (err) => {
            if(err) console.error(err);
            completed++;
            if(completed === stocks.length) broadcastStocks();
        });
    });

    res.send({ status: "ok" });
});

// --- Clear live stocks ---
app.post("/clear-live", (req, res) => {
    const todayStr = new Date().toISOString().slice(0,10);
    db.run("DELETE FROM stocks WHERE datetime LIKE ?", [`${todayStr}%`], (err) => {
        if(err) console.error(err);
        broadcastStocks();
        res.send({ status: "ok" });
    });
});

// --- Clear history stocks ---
app.post("/clear-history", (req, res) => {
    const todayStr = new Date().toISOString().slice(0,10);
    db.run("DELETE FROM stocks WHERE datetime NOT LIKE ?", [`${todayStr}%`], (err) => {
        if(err) console.error(err);
        broadcastStocks();
        res.send({ status: "ok" });
    });
});

// --- Cleanup older than 5 days ---
setInterval(() => {
    const fiveDaysAgo = new Date(Date.now() - 5*24*60*60*1000).toISOString().slice(0,10);
    db.run("DELETE FROM stocks WHERE datetime < ?", [fiveDaysAgo], (err)=>{
        if(err) console.error(err);
        broadcastStocks();
    });
}, 1000*60*60); // every hour

// --- Start server ---
const PORT = process.env.PORT || 5000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
