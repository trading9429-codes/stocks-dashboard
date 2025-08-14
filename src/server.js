const express = require("express");
const WebSocket = require("ws");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const bodyParser = require("body-parser");
const http = require("http");

const app = express();
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));

const db = new sqlite3.Database("stocks.db");
db.run(`CREATE TABLE IF NOT EXISTS stocks (
    stock TEXT PRIMARY KEY,
    trigger_price REAL,
    date TEXT,
    time TEXT,
    count INTEGER,
    lastUpdated TEXT,
    scan_name TEXT,
    scan_url TEXT,
    alert_name TEXT
)`);

// Helpers
function formatDateTime(date){
    let dd = String(date.getDate()).padStart(2,'0');
    let mm = String(date.getMonth()+1).padStart(2,'0');
    let yyyy = date.getFullYear();
    let hours = date.getHours();
    let minutes = String(date.getMinutes()).padStart(2,'0');
    const ampm = hours >= 12 ? 'PM' : 'AM';
    hours = hours % 12 || 12;
    return `${dd}/${mm}/${yyyy} ${hours}:${minutes} ${ampm}`;
}
function formatTime(date){
    let hours = date.getHours();
    let minutes = String(date.getMinutes()).padStart(2,'0');
    const ampm = hours >= 12 ? 'PM' : 'AM';
    hours = hours % 12 || 12;
    return `${hours}:${minutes} ${ampm}`;
}
function isToday(dateStr){
    const [dd,mm,yyyy] = dateStr.split("/").map(Number);
    const stockDate = new Date(yyyy, mm-1, dd);
    const today = new Date();
    return stockDate.toDateString() === today.toDateString();
}
function isLast5Days(dateStr){
    const [dd,mm,yyyy] = dateStr.split("/").map(Number);
    const stockDate = new Date(yyyy, mm-1, dd);
    const today = new Date();
    const diff = (today - stockDate) / (1000*60*60*24);
    return diff >=1 && diff <=5;
}
function timeToMinutes(str){
    const [time, ampm] = str.split(" ");
    let [h,m] = time.split(":").map(Number);
    if(ampm.toUpperCase() === 'PM' && h<12) h+=12;
    if(ampm.toUpperCase() === 'AM' && h===12) h=0;
    return h*60 + m;
}

// HTTP + WebSocket server
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Broadcast stocks
function broadcastStocks() {
    db.all("SELECT * FROM stocks", [], (err, rows) => {
        if (err) return console.error(err);
        const liveStocks = rows.filter(s => isToday(s.date))
                               .sort((a,b) => timeToMinutes(a.time) - timeToMinutes(b.time));
        const historyStocks = rows.filter(s => isLast5Days(s.date))
                                  .sort((a,b) => {
                                      const [dd1, mm1, yyyy1] = a.date.split("/").map(Number);
                                      const [dd2, mm2, yyyy2] = b.date.split("/").map(Number);
                                      return new Date(yyyy2, mm2-1, dd2) - new Date(yyyy1, mm1-1, dd1);
                                  });
        wss.clients.forEach(client => {
            if(client.readyState === WebSocket.OPEN){
                client.send(JSON.stringify({ liveStocks, historyStocks }));
            }
        });
    });
}

// WebSocket connection
wss.on("connection", ws => {
    console.log("Client connected");
    broadcastStocks();
});

// New stocks from n8n
app.post("/new-stocks", (req,res) => {
    const stocks = req.body[0]?.data || [];
    const today = new Date();
    const dateStr = `${String(today.getDate()).padStart(2,'0')}/${String(today.getMonth()+1).padStart(2,'0')}/${today.getFullYear()}`;

    let completed = 0;
    if(stocks.length === 0) return res.send({status:"ok"});

    stocks.forEach(item => {
        const timeStr = item.triggered_at || formatTime(new Date());
        const lastUpdated = formatDateTime(new Date());

        db.get("SELECT * FROM stocks WHERE stock=?", [item.stock], (err,row)=>{
            if(err) console.error(err);

            const callback = () => {
                completed++;
                if(completed === stocks.length) broadcastStocks();
            };

            if(row){
                db.run(`UPDATE stocks SET 
                    trigger_price=?, count=?, lastUpdated=?, scan_name=?, scan_url=?, alert_name=?, time=?
                    WHERE stock=?`,
                    [item.trigger_price, row.count+1, lastUpdated, item.scan_name, item.scan_url, item.alert_name, timeStr, item.stock],
                    callback
                );
            } else {
                db.run(`INSERT INTO stocks 
                    (stock, trigger_price, date, time, count, lastUpdated, scan_name, scan_url, alert_name)
                    VALUES (?,?,?,?,?,?,?,?,?)`,
                    [item.stock, item.trigger_price, dateStr, timeStr, 1, lastUpdated, item.scan_name, item.scan_url, item.alert_name],
                    callback
                );
            }
        });
    });

    res.send({status:"ok"});
});

// Clear Live stocks
app.post("/clear-live", (req,res)=>{
    const today = new Date();
    const dateStr = `${String(today.getDate()).padStart(2,'0')}/${String(today.getMonth()+1).padStart(2,'0')}/${today.getFullYear()}`;
    db.run("DELETE FROM stocks WHERE date=?", [dateStr], err=>{
        if(err) console.error(err);
        broadcastStocks();
        res.send({status:"ok"});
    });
});

// Clear History stocks
app.post("/clear-history", (req,res)=>{
    const today = new Date();
    const dateStr = `${String(today.getDate()).padStart(2,'0')}/${String(today.getMonth()+1).padStart(2,'0')}/${today.getFullYear()}`;
    db.run("DELETE FROM stocks WHERE date<>?", [dateStr], err=>{
        if(err) console.error(err);
        broadcastStocks();
        res.send({status:"ok"});
    });
});

// Cleanup older than 5 days
setInterval(()=>{
    db.all("SELECT stock, date FROM stocks", [], (err, rows)=>{
        if(err) return;
        const today = new Date();
        rows.forEach(row=>{
            const [dd,mm,yyyy] = row.date.split("/").map(Number);
            const stockDate = new Date(yyyy, mm-1, dd);
            const diff = (today - stockDate)/(1000*60*60*24);
            if(diff>5) db.run("DELETE FROM stocks WHERE stock=?", [row.stock]);
        });
        broadcastStocks();
    });
}, 1000*60*60);

// Listen
const PORT = process.env.PORT || 5000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
