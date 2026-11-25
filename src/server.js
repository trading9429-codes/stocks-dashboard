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

// --- CORS Middleware (allows cross-origin requests from Chartink Tampermonkey userscripts) ---
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, x-webhook-secret");
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

// Disable caching for HTML, Service Worker & Manifest so client apps & WebViews always receive latest updates
app.use(express.static(path.join(__dirname, "public"), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith("index.html") || filePath.endsWith("sw.js") || filePath.endsWith("manifest.json")) {
      res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
      res.setHeader("Pragma", "no-cache");
      res.setHeader("Expires", "0");
    }
  }
}));

app.get("/", (req, res) => {
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

// --- Web Push & VAPID Configuration ---
let webpush = null;
let currentVapidKeys = {
  publicKey: process.env.VAPID_PUBLIC_KEY || null,
  privateKey: process.env.VAPID_PRIVATE_KEY || null,
};
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || "mailto:admin@stocks-dashboard.local";

try {
  webpush = require("web-push");
  if (currentVapidKeys.publicKey && currentVapidKeys.privateKey) {
    webpush.setVapidDetails(VAPID_SUBJECT, currentVapidKeys.publicKey, currentVapidKeys.privateKey);
    console.log("[push] VAPID configured from environment variables");
  } else {
    const generated = webpush.generateVAPIDKeys();
    currentVapidKeys = generated;
    webpush.setVapidDetails(VAPID_SUBJECT, generated.publicKey, generated.privateKey);
    console.log("[push] Auto-generated session VAPID keys for mobile push");
  }
} catch (_) {
  console.log("[push] web-push module not loaded. Run 'npm install web-push' to enable background Web Push.");
}

const pushSubscriptions = new Map(); // endpoint -> subscription

// --- Market Breadth State ---
let breadthState = {
  ratio: null,
  status: "UNKNOWN", // "GO" | "NO_GO" | "UNKNOWN"
  threshold: 350,
  recorded_at: null,
  time_ist: null,
  source: null,
  lastCheck: null,
};

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
      stock         TEXT PRIMARY KEY,
      trigger_price REAL,
      datetime      TIMESTAMPTZ,
      count         INTEGER,
      lastUpdated   TIMESTAMPTZ,
      scan_name     TEXT,
      scan_url      TEXT,
      alert_name    TEXT,
      scans         JSONB DEFAULT '[]'::jsonb,
      sector        TEXT,
      rel_vol       REAL,
      pct_change    REAL,
      adr_pct       REAL,
      extra_data    JSONB DEFAULT '{}'::jsonb
    )
  `);

  // Safe incremental migrations for existing tables
  await db.query(`
    ALTER TABLE stocks ADD COLUMN IF NOT EXISTS scans JSONB DEFAULT '[]'::jsonb;
    ALTER TABLE stocks ADD COLUMN IF NOT EXISTS sector TEXT;
    ALTER TABLE stocks ADD COLUMN IF NOT EXISTS rel_vol REAL;
    ALTER TABLE stocks ADD COLUMN IF NOT EXISTS pct_change REAL;
    ALTER TABLE stocks ADD COLUMN IF NOT EXISTS adr_pct REAL;
    ALTER TABLE stocks ADD COLUMN IF NOT EXISTS extra_data JSONB DEFAULT '{}'::jsonb;
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS push_subscriptions (
      endpoint     TEXT PRIMARY KEY,
      subscription JSONB NOT NULL,
      created_at   TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_datetime_stock ON stocks (datetime, stock)`
  );

  await db.query(`
    CREATE TABLE IF NOT EXISTS market_breadth (
      id SERIAL PRIMARY KEY,
      ratio REAL NOT NULL,
      status TEXT NOT NULL,
      threshold REAL DEFAULT 350,
      source TEXT DEFAULT 'chartink-dashboard-508179',
      recorded_at TIMESTAMPTZ DEFAULT NOW(),
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_breadth_recorded ON market_breadth (recorded_at DESC)
  `);

  // Load latest breadth reading
  try {
    const { rows } = await db.query(`
      SELECT ratio, status, threshold, source,
             to_char(recorded_at AT TIME ZONE '${TZ}', 'HH12:MI AM') AS time_ist,
             recorded_at
        FROM market_breadth
       ORDER BY recorded_at DESC LIMIT 1
    `);
    if (rows.length) {
      breadthState.ratio = parseFloat(rows[0].ratio);
      breadthState.status = rows[0].status;
      breadthState.threshold = parseFloat(rows[0].threshold || 350);
      breadthState.source = rows[0].source;
      breadthState.time_ist = rows[0].time_ist;
      breadthState.recorded_at = rows[0].recorded_at;
      console.log(`[breadth] loaded latest breadth state: ratio=${breadthState.ratio}, status=${breadthState.status}`);
    }
  } catch (err) {
    console.warn("[breadth] warning initializing breadth cache:", err.message);
  }

  // Load active push subscriptions into memory
  try {
    const res = await db.query(`SELECT subscription FROM push_subscriptions`);
    res.rows.forEach(r => {
      const sub = r.subscription;
      if (sub && sub.endpoint) {
        pushSubscriptions.set(sub.endpoint, sub);
      }
    });
    console.log(`[push] loaded ${pushSubscriptions.size} push subscriptions`);
  } catch (err) {
    console.warn("[push] could not load subscriptions from db:", err.message);
  }

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
  // Returns true if current IST time is within active market trading hours (9:15 AM – 3:30 PM IST, Mon–Fri)
  isMarketHoursIST: () => {
    const now = moment().tz(TZ);
    const day = now.day(); // 0=Sun … 6=Sat
    if (day === 0 || day === 6) return false;
    const mins = now.hours() * 60 + now.minutes();
    return mins >= 9 * 60 + 15 && mins <= 15 * 60 + 30;
  },
};

// --- IST projection SQL fragment ---
const SELECT_IST = `
  stock, trigger_price, count, scan_name, scan_url, alert_name,
  sector, rel_vol, pct_change, adr_pct, scans, extra_data,
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
      const b = i * 14;
      parts.push(
        `($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},$${b+7},$${b+8},$${b+9}::jsonb,$${b+10},$${b+11},$${b+12},$${b+13},$${b+14}::jsonb)`
      );
      vals.push(
        it.stock,
        it.trigger_price,
        it.datetimeUTC,
        1,
        it.lastUpdatedUTC,
        it.scan_name || null,
        it.scan_url || null,
        it.alert_name || null,
        JSON.stringify(it.scans || []),
        it.sector || null,
        it.rel_vol != null ? it.rel_vol : null,
        it.pct_change != null ? it.pct_change : null,
        it.adr_pct != null ? it.adr_pct : null,
        JSON.stringify(it.extra_data || {})
      );
    });

    const { rows } = await db.query(
      `WITH upserted AS (
         INSERT INTO stocks
           (stock, trigger_price, datetime, count, lastUpdated, scan_name, scan_url, alert_name, scans, sector, rel_vol, pct_change, adr_pct, extra_data)
         VALUES ${parts.join(",")}
         ON CONFLICT (stock) DO UPDATE SET
           trigger_price = EXCLUDED.trigger_price,
           count         = CASE
                             -- If existing record was from a previous day (IST), reset count to 1 for today
                             WHEN date_trunc('day', stocks.datetime AT TIME ZONE '${TZ}') != date_trunc('day', EXCLUDED.datetime AT TIME ZONE '${TZ}')
                               THEN 1
                             -- If alert arrived at the same time (same minute IST or within 60 seconds), do not increment count
                             WHEN date_trunc('minute', stocks.datetime AT TIME ZONE '${TZ}') = date_trunc('minute', EXCLUDED.datetime AT TIME ZONE '${TZ}')
                               OR abs(EXTRACT(EPOCH FROM (EXCLUDED.datetime - stocks.datetime))) < 60
                               THEN stocks.count
                             -- Different candle / later time today: increment trigger count
                             ELSE stocks.count + 1
                           END,
           datetime      = CASE
                             WHEN date_trunc('day', stocks.datetime AT TIME ZONE '${TZ}') != date_trunc('day', EXCLUDED.datetime AT TIME ZONE '${TZ}')
                               THEN EXCLUDED.datetime
                             ELSE GREATEST(stocks.datetime, EXCLUDED.datetime)
                           END,
           lastUpdated   = EXCLUDED.lastUpdated,
           scan_name     = EXCLUDED.scan_name,
           scan_url      = EXCLUDED.scan_url,
           alert_name    = EXCLUDED.alert_name,
           scans         = CASE
                             -- New trading day: start fresh with today's scan
                             WHEN date_trunc('day', stocks.datetime AT TIME ZONE '${TZ}') != date_trunc('day', EXCLUDED.datetime AT TIME ZONE '${TZ}')
                               THEN CASE WHEN EXCLUDED.scan_name IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(EXCLUDED.scan_name) END
                             WHEN EXCLUDED.scan_name IS NULL THEN COALESCE(stocks.scans, '[]'::jsonb)
                             WHEN COALESCE(stocks.scans, '[]'::jsonb) @> jsonb_build_array(EXCLUDED.scan_name) THEN COALESCE(stocks.scans, '[]'::jsonb)
                             ELSE COALESCE(stocks.scans, '[]'::jsonb) || jsonb_build_array(EXCLUDED.scan_name)
                           END,
           sector        = COALESCE(EXCLUDED.sector, stocks.sector),
           rel_vol       = COALESCE(EXCLUDED.rel_vol, stocks.rel_vol),
           pct_change    = COALESCE(EXCLUDED.pct_change, stocks.pct_change),
           adr_pct       = COALESCE(EXCLUDED.adr_pct, stocks.adr_pct),
           extra_data    = COALESCE(stocks.extra_data, '{}'::jsonb) || EXCLUDED.extra_data
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

function broadcastBreadth() {
  const payload = JSON.stringify({ type: "breadth", breadth: breadthState });
  wss.clients.forEach((c) => c.readyState === WebSocket.OPEN && c.send(payload));
}

async function refreshCache(full = false, deltaLive = []) {
  const broadcast = (payload) =>
    wss.clients.forEach((c) => c.readyState === WebSocket.OPEN && c.send(payload));

  if (full) {
    const { startUTC, endUTC }  = timeUtils.dayBoundsUTC();
    const fiveDaysAgoUTC = moment(startUTC).subtract(RETENTION_DAYS, "days").toDate();

    const [live, history] = await Promise.all([
      dbUtils.getRangeIST(startUTC, endUTC),
      dbUtils.getRangeIST(fiveDaysAgoUTC, startUTC),
    ]);
    cache.live    = live;
    cache.history = history;

    broadcast(JSON.stringify({
      type: "init",
      liveStocks: live,
      historyStocks: history,
      breadth: breadthState,
    }));
  } else if (deltaLive.length) {
    // Fast in-memory update without hitting PostgreSQL on every webhook delta
    const byStock = new Map(cache.live.map((item) => [item.stock, item]));
    deltaLive.forEach((row) => {
      byStock.set(row.stock, row);
    });
    // Keep sorted by latest timestamp descending
    cache.live = Array.from(byStock.values()).sort((a, b) => (b.ts_ist ?? 0) - (a.ts_ist ?? 0));

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

// Helper to safely extract column values regardless of case, spacing, dashes, or percentage signs
function extractColumnValue(obj, ...possibleKeys) {
  if (!obj || typeof obj !== "object") return undefined;
  for (const target of possibleKeys) {
    const cleanTarget = target.toLowerCase().replace(/[\s_\-%/]/g, "");
    for (const k of Object.keys(obj)) {
      const cleanK = k.toLowerCase().replace(/[\s_\-%/]/g, "");
      if (cleanK === cleanTarget) return obj[k];
    }
  }
  return undefined;
}

// --- Comprehensive NSE Sector Master Map for Indian Equities ---
const NSE_SECTOR_MAP = {
  // Banking
  "HDFCBANK": "Banking", "ICICIBANK": "Banking", "SBIN": "Banking", "KOTAKBANK": "Banking", "AXISBANK": "Banking",
  "INDUSINDBK": "Banking", "BANKBARODA": "Banking", "PNB": "Banking", "CANBK": "Banking", "FEDERALBNK": "Banking",
  "IDFCFIRSTB": "Banking", "AUBANK": "Banking", "BANDHANBNK": "Banking", "RBLBANK": "Banking", "UNIONBANK": "Banking",
  "IOB": "Banking", "UCOBANK": "Banking", "CENTRALBK": "Banking", "BANKINDIA": "Banking", "MAHABANK": "Banking",
  "KARURVYSYA": "Banking", "CUB": "Banking", "SOUTHBANK": "Banking", "YESBANK": "Banking", "PSB": "Banking",

  // IT & Tech
  "TCS": "IT", "INFY": "IT", "HCLTECH": "IT", "WIPRO": "IT", "TECHM": "IT", "LTIM": "IT",
  "COFORGE": "IT", "PERSISTENT": "IT", "MPHASIS": "IT", "KPITTECH": "IT", "TATAELXSI": "IT", "LTTS": "IT",
  "OFSS": "IT", "CYIENT": "IT", "SONATSOFTW": "IT", "BSOFT": "IT", "ZENSARTECH": "IT", "INTELLECT": "IT",
  "MASTEK": "IT", "HEXAWARE": "IT", "HAPPSTMNDS": "IT", "ROUTE": "IT", "TANLA": "IT", "AFFLE": "IT", "RATEGAIN": "IT",

  // Auto & Auto Ancillaries
  "MARUTI": "Auto", "TATAMOTORS": "Auto", "M&M": "Auto", "BAJAJ-AUTO": "Auto", "HEROMOTOCO": "Auto",
  "EICHERMOT": "Auto", "TVSMOTOR": "Auto", "BHARATFORG": "Auto", "ASHOKLEY": "Auto", "BALKRISIND": "Auto",
  "APOLLOTYRE": "Auto", "MRF": "Auto", "MOTHERSON": "Auto", "BOSCHLTD": "Auto", "SONACOMS": "Auto",
  "TIINDIA": "Auto", "EXIDEIND": "Auto", "AMARAJABAT": "Auto", "ARE&M": "Auto", "ENDURANCE": "Auto",
  "CRAFTSMAN": "Auto", "CEATLTD": "Auto", "JKTYRE": "Auto", "GABRIEL": "Auto", "SUBROS": "Auto", "ESCORTS": "Auto",

  // Pharma & Healthcare
  "SUNPHARMA": "Pharma", "DRREDDY": "Pharma", "CIPLA": "Pharma", "DIVISLAB": "Pharma", "APOLLOHOSP": "Pharma",
  "LUPIN": "Pharma", "AUROPHARMA": "Pharma", "BIOCON": "Pharma", "TORNTPHARM": "Pharma", "ALKEM": "Pharma",
  "ZYDUSLIFE": "Pharma", "MANKIND": "Pharma", "MAXHEALTH": "Pharma", "FORTIS": "Pharma", "GLENMARK": "Pharma",
  "GRANULES": "Pharma", "IPCALAB": "Pharma", "SYNGENE": "Pharma", "LAURUSLABS": "Pharma", "GLAND": "Pharma",
  "ABBOTINDIA": "Pharma", "PFIZER": "Pharma", "SANOFI": "Pharma", "GLAXO": "Pharma", "AJANTPHARM": "Pharma",
  "NATCOPHARM": "Pharma", "JBCHEPHARM": "Pharma", "ERIS": "Pharma", "MEDANTA": "Pharma", "NH": "Pharma",
  "KIMS": "Pharma", "RAINBOW": "Pharma", "METROPOLIS": "Pharma", "LALPATHLAB": "Pharma", "SUVENPHAR": "Pharma",

  // Energy, Oil & Gas, Power
  "RELIANCE": "Energy", "ONGC": "Energy", "NTPC": "Energy", "POWERGRID": "Energy", "BPCL": "Energy",
  "IOC": "Energy", "GAIL": "Energy", "TATAPOWER": "Energy", "ADANIGREEN": "Energy", "ADANIPOWER": "Energy",
  "ADANIENSOL": "Energy", "JSWENERGY": "Energy", "COALINDIA": "Energy", "PETRONET": "Energy", "IGL": "Energy",
  "MGL": "Energy", "GUJGASLTD": "Energy", "OIL": "Energy", "HINDPETRO": "Energy", "NHPC": "Energy",
  "SJVN": "Energy", "TORNTPOWER": "Energy", "CESC": "Energy", "SUZLON": "Energy", "INOXWIND": "Energy",
  "IREDA": "Energy", "GIPCL": "Energy",

  // FMCG & Consumption
  "ITC": "FMCG", "HINDUNILVR": "FMCG", "NESTLEIND": "FMCG", "BRITANNIA": "FMCG", "TATACONSUM": "FMCG",
  "DABUR": "FMCG", "MARICO": "FMCG", "GODREJCP": "FMCG", "COLPAL": "FMCG", "VBL": "FMCG",
  "RADICO": "FMCG", "UBL": "FMCG", "MCDOWELL-N": "FMCG", "PGHH": "FMCG", "EMAMILTD": "FMCG",
  "BALRAMCHIN": "FMCG", "RENUKA": "FMCG", "EIDPARRY": "FMCG", "BIKAJI": "FMCG", "PATANJALI": "FMCG",
  "HONASA": "FMCG", "DEVYANI": "FMCG", "JUBLFOOD": "FMCG", "WESTLIFE": "FMCG", "SAPPHIRE": "FMCG",

  // Metals & Mining
  "TATASTEEL": "Metals", "JSWSTEEL": "Metals", "HINDALCO": "Metals", "JINDALSTEL": "Metals", "VEDL": "Metals",
  "NMDC": "Metals", "SAIL": "Metals", "NATIONALUM": "Metals", "HINDZINC": "Metals", "APLAPOLLO": "Metals",
  "RATNAMANI": "Metals", "WELCORP": "Metals", "JINDALSAW": "Metals", "HINDCOPPER": "Metals", "KIOCL": "Metals",

  // Financial Services & NBFC & Insurance
  "BAJFINANCE": "Financial Services", "BAJAJFINSV": "Financial Services", "CHOLAFIN": "Financial Services",
  "SHRIRAMFIN": "Financial Services", "MUTHOOTFIN": "Financial Services", "MANAPPURAM": "Financial Services",
  "PFC": "Financial Services", "RECLTD": "Financial Services", "LICHSGFIN": "Financial Services",
  "HDFCLIFE": "Financial Services", "SBILIFE": "Financial Services", "ICICIPRULI": "Financial Services",
  "ICICIGI": "Financial Services", "HDFCAMC": "Financial Services", "NAM-INDIA": "Financial Services",
  "SBICARD": "Financial Services", "LICI": "Financial Services", "JIOFIN": "Financial Services",
  "POONAWALLA": "Financial Services", "M&MFIN": "Financial Services", "SUNDARMFIN": "Financial Services",
  "CANFINHOME": "Financial Services", "HUDCO": "Financial Services", "IIFL": "Financial Services",
  "ANGELONE": "Financial Services", "MOTILALOFS": "Financial Services", "BSE": "Financial Services",
  "MCX": "Financial Services", "CDSL": "Financial Services", "CAMS": "Financial Services", "KFINTECH": "Financial Services",

  // Realty & Construction
  "DLF": "Realty", "GODREJPROP": "Realty", "LODHA": "Realty", "OBEROIRLTY": "Realty", "PHOENIXLTD": "Realty",
  "BRIGADE": "Realty", "PRESTIGE": "Realty", "SOBHA": "Realty", "NBCC": "Realty", "SUNTECK": "Realty",
  "MAHLIFE": "Realty", "SIGNATURE": "Realty", "IBREALEST": "Realty", "KOLTEPATIL": "Realty",

  // Capital Goods, Defence, Infra & Engineering
  "LT": "Capital Goods", "HAL": "Defence", "BEL": "Defence", "BHEL": "Capital Goods", "SIEMENS": "Capital Goods",
  "ABB": "Capital Goods", "CUMMINSIND": "Capital Goods", "HAVELLS": "Capital Goods", "POLYCAB": "Capital Goods",
  "KEI": "Capital Goods", "ASTRAL": "Capital Goods", "VOLTAS": "Capital Goods", "BLUESTARCO": "Capital Goods",
  "CGPOWER": "Capital Goods", "MAZDOCK": "Defence", "COCHINSHIP": "Defence", "GRSE": "Defence", "BDL": "Defence",
  "KAYNES": "Capital Goods", "DIXON": "Capital Goods", "AMBER": "Capital Goods", "TITAGARH": "Capital Goods",
  "RAILTEL": "Infra", "RITES": "Infra", "IRFC": "Financial Services", "RVNL": "Infra", "IRCON": "Infra",
  "GMRINFRA": "Infra", "ADANIPORTS": "Infra", "ADANIENT": "Conglomerate", "BEML": "Defence", "THERMAX": "Capital Goods",

  // Chemicals & Fertilizers
  "PIDILITIND": "Chemicals", "SRF": "Chemicals", "DEEPAKNTR": "Chemicals", "TATACHEM": "Chemicals",
  "AARTIIND": "Chemicals", "PIIND": "Chemicals", "UPL": "Chemicals", "COROMANDEL": "Chemicals",
  "CHAMBLFERT": "Chemicals", "GNFC": "Chemicals", "ATUL": "Chemicals", "NAVINFLUOR": "Chemicals",
  "FLUOROCHEM": "Chemicals", "SUMICHEM": "Chemicals", "FINEORG": "Chemicals", "CLEAN": "Chemicals",
  "ALKYLAMINE": "Chemicals", "BALAMINES": "Chemicals", "AETHER": "Chemicals", "ANUPAM": "Chemicals",

  // Cement & Building Materials
  "ULTRACEMCO": "Cement", "GRASIM": "Cement", "AMBUJACEM": "Cement", "ACC": "Cement", "DALBHARAT": "Cement",
  "SHREECEM": "Cement", "JKCEMENT": "Cement", "RAMCOCEM": "Cement", "SUPREMEIND": "Building Materials",
  "FINPIPE": "Building Materials", "PRINCEPIPE": "Building Materials", "KAJARIACER": "Building Materials",
  "CENTURYPLY": "Building Materials",

  // Telecom & Media
  "BHARTIARTL": "Telecom", "IDEA": "Telecom", "INDUSTOWER": "Telecom", "TATACOMM": "Telecom",
  "ZEEL": "Media", "SUNTV": "Media", "PVRINOX": "Media", "TV18BRDCST": "Media", "NETWORK18": "Media",

  // Retail, Aviation & Services
  "INDIGO": "Aviation", "SPICEJET": "Aviation", "DMART": "Retail", "TRENT": "Retail", "TITAN": "Retail",
  "KALYANKJIL": "Retail", "SENCO": "Retail", "BATAINDIA": "Retail", "PAGEIND": "Retail", "MANYAVAR": "Retail",
  "ZOMATO": "Consumer Tech", "SWIGGY": "Consumer Tech", "NYKAA": "Consumer Tech", "DELHIVERY": "Logistics",
  "CONCOR": "Logistics", "BLUEDART": "Logistics", "TCIEXP": "Logistics", "IRCTC": "Consumer Services",
  "INDHOTEL": "Hospitality", "EIHOTEL": "Hospitality", "LEMONTREE": "Hospitality"
};

function getFallbackSector(stockSymbol) {
  if (!stockSymbol) return null;
  const clean = String(stockSymbol)
    .trim()
    .toUpperCase()
    .replace(/-EQ$/, "")
    .replace(/\.NS$/, "")
    .replace(/[^A-Z0-9&_\-]/g, "");
  return NSE_SECTOR_MAP[clean] || null;
}

// --- Transform Chartink webhook payload ---
function transform(body) {
  const { stocks, trigger_prices, triggered_at, scan_name, scan_url, alert_name, columns } = body;
  if (!stocks || !trigger_prices) return [];

  const stockArr = String(stocks).split(",").map((s) => s.trim()).filter(Boolean);
  const priceArr = String(trigger_prices).split(",").map((p) => parseFloat(p.trim()));
  const nowUTC   = timeUtils.nowUTC();

  // Deduplicate symbols within the same payload while preserving trigger prices
  const seenSymbols = new Set();
  const dedupedStocks = [];
  stockArr.forEach((s, i) => {
    const sym = s.toUpperCase();
    if (!seenSymbols.has(sym)) {
      seenSymbols.add(sym);
      dedupedStocks.push({ stock: s, price: priceArr[i] });
    }
  });

  // Index custom columns by symbol
  const colMap = new Map();
  if (Array.isArray(columns)) {
    columns.forEach((col) => {
      if (col && col.symbol) {
        colMap.set(String(col.symbol).trim().toUpperCase(), col);
      }
    });
  }

  return dedupedStocks
    .map(({ stock: s, price: p }) => {
      const colData = colMap.get(s.toUpperCase()) || {};

      // Parse Relative Volume (handles 'Rel Vol', 'rel_vol', 'Relative Volume', etc.)
      const rawRv = extractColumnValue(colData, "Rel Vol", "rel_vol", "relvol", "relativevolume");
      let relVol = Number.isFinite(parseFloat(rawRv)) ? parseFloat(rawRv) : null;
      if (relVol !== null) {
        // Auto-detect percentage scale from Chartink scans (e.g. 100% -> 1.0x, 250% -> 2.5x)
        if (relVol >= 25 && relVol < 2000) {
          relVol = Math.round((relVol / 100) * 100) / 100;
        }
      }

      // Parse Sector with flexible key matching and NSE Master fallback
      const rawSector = extractColumnValue(
        colData,
        "sector",
        "sector_name",
        "sectorname",
        "industry",
        "industry_name",
        "group"
      );
      const fallbackSector = getFallbackSector(s);
      const sector = (rawSector != null && String(rawSector).trim() !== "" && String(rawSector).trim() !== "—" && String(rawSector).trim() !== "-")
        ? String(rawSector).trim()
        : fallbackSector;

      // Parse % Change (handles '%_change', 'change', 'pct_change', 'chg', etc.)
      const rawPct = extractColumnValue(colData, "%_change", "change", "pct_change", "pctchange", "chg");
      const pctChange = Number.isFinite(parseFloat(rawPct)) ? parseFloat(rawPct) : null;

      // Parse ADR-H/L% (handles 'ADR-H/L%', 'adrhl', 'adr', 'adr_pct', etc.)
      const rawAdr = extractColumnValue(colData, "ADR-H/L%", "adrhl", "adr", "adr_pct");
      const adrPct = Number.isFinite(parseFloat(rawAdr)) ? parseFloat(rawAdr) : null;

      return {
        stock:          s,
        trigger_price:  Number.isFinite(p) ? p : null,
        datetimeUTC:    triggered_at ? timeUtils.parseISTTimeToUTC(triggered_at) : nowUTC,
        lastUpdatedUTC: nowUTC,
        scan_name,
        scan_url,
        alert_name,
        scans:          scan_name ? [scan_name] : [],
        sector,
        rel_vol:        relVol,
        pct_change:     pctChange,
        adr_pct:        adrPct,
        extra_data:     colData,
      };
    })
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
    breadth: breadthState,
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

// --- Telegram Mobile Push Notification (Optional) ---
function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function formatTelegramMessage(it) {
  const isNew = Number(it.count || 1) <= 1;
  const headline = isNew ? "🟢 <b>NEW CHARTINK ALERT</b>" : `🟡 <b>STOCK UPDATE (Count: ${escapeHtml(it.count)})</b>`;
  const price = it.trigger_price ? `₹${Number(it.trigger_price).toFixed(2)}` : "—";
  const scan = it.scan_name ? `\n🔍 <b>Scan:</b> ${escapeHtml(it.scan_name)}` : "";
  const time = it.time_ist ? `\n⏱ <b>Time:</b> ${escapeHtml(it.time_ist)} IST` : "";

  let relVolStr = "";
  if (it.rel_vol != null && !isNaN(Number(it.rel_vol))) {
    let rv = Number(it.rel_vol);
    if (rv >= 25 && rv < 2000) rv = Math.round((rv / 100) * 100) / 100;
    if (rv >= 2000) {
      const formatted = rv >= 1e6 ? (rv / 1e6).toFixed(1) + "M" : Math.round(rv / 1e3) + "K";
      relVolStr = `\n📊 <b>Volume:</b> ${formatted} shares`;
    } else {
      const fire = rv >= 2.0 ? " 🔥" : "";
      relVolStr = `\n📊 <b>Rel Vol:</b> ${rv.toFixed(2)}x${fire}`;
    }
  }

  let metricsStr = "";
  if (it.pct_change != null && !isNaN(Number(it.pct_change))) {
    const chg = Number(it.pct_change);
    const sign = chg > 0 ? "+" : "";
    metricsStr += ` | <b>Chg:</b> ${sign}${chg.toFixed(2)}%`;
  }
  if (it.adr_pct != null && !isNaN(Number(it.adr_pct))) {
    metricsStr += ` | <b>ADR:</b> ${Number(it.adr_pct).toFixed(2)}%`;
  }

  const sectorStr = it.sector ? `\n🏢 <b>Sector:</b> ${escapeHtml(it.sector)}` : "";

  let confluenceStr = "";
  if (Array.isArray(it.scans) && it.scans.length > 1) {
    confluenceStr = `\n⚡ <b>Confluence (${it.scans.length} scans):</b> ${it.scans.map((s) => escapeHtml(s)).join(", ")}`;
  }

  const chartUrl = `https://in.tradingview.com/chart/?symbol=NSE:${encodeURIComponent(it.stock)}`;
  const fyersUrl = `https://trade.fyers.in/?symbol=NSE:${encodeURIComponent(it.stock)}-EQ`;
  const dhanUrl  = `https://web.dhan.co/index/order?symbol=NSE:${encodeURIComponent(it.stock)}&type=BUY`;

  return `${headline}\n📈 <b>${escapeHtml(it.stock)}</b> @ ${price}${metricsStr}${relVolStr}${sectorStr}${time}${scan}${confluenceStr}\n🔗 <a href="${chartUrl}">TradingView</a> | <a href="${fyersUrl}">⚡ Fyers</a> | <a href="${dhanUrl}">⚡ Dhan</a>`;
}

function sendTelegramRaw(text) {
  return new Promise((resolve, reject) => {
    const token  = process.env.TELEGRAM_BOT_TOKEN;
    const chatId = process.env.TELEGRAM_CHAT_ID;
    if (!token || !chatId) {
      return reject(new Error("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not configured"));
    }

    const payload = JSON.stringify({
      chat_id: chatId,
      text: text,
      parse_mode: "HTML",
      disable_web_page_preview: true,
    });

    const req = https.request(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(payload),
        },
      },
      (res) => {
        let body = "";
        res.on("data", (chunk) => (body += chunk));
        res.on("end", () => {
          try {
            const json = JSON.parse(body);
            if (res.statusCode === 200 && json.ok) {
              console.log("[telegram] message delivered successfully");
              resolve(json);
            } else {
              console.error(`[telegram] API error (${res.statusCode}):`, body);
              const err = new Error(`Telegram error (${res.statusCode}): ${json.description || body}`);
              err.details = json;
              reject(err);
            }
          } catch (_) {
            reject(new Error(`Telegram response error (${res.statusCode}): ${body}`));
          }
        });
      }
    );

    req.on("error", (err) => {
      console.error("[telegram] network error:", err.message);
      reject(err);
    });

    req.write(payload);
    req.end();
  });
}

function notifyTelegram(items) {
  const token  = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) {
    return;
  }
  if (!items?.length) return;

  items.forEach((it) => {
    const text = formatTelegramMessage(it);
    sendTelegramRaw(text).catch((err) => {
      console.error(`[telegram] failed to send alert for ${it.stock}:`, err.message);
    });
  });
}

// Mobile Web Push Broadcaster
async function notifyPush(items) {
  if (!webpush || !pushSubscriptions.size || !items?.length) return;

  for (const it of items) {
    const isHighVol = Number(it.rel_vol) >= 2.0;
    const volTag = (it.rel_vol != null && !isNaN(Number(it.rel_vol)))
      ? ` · Vol: ${Number(it.rel_vol).toFixed(1)}x${isHighVol ? '🔥' : ''}`
      : '';
    const chgTag = (it.pct_change != null && !isNaN(Number(it.pct_change)))
      ? ` · ${Number(it.pct_change) > 0 ? '+' : ''}${Number(it.pct_change).toFixed(2)}%`
      : '';

    const title = `🟢 ${it.stock} @ ₹${Number(it.trigger_price || 0).toFixed(2)}${chgTag}`;
    const body = `${it.sector ? 'Sector: ' + it.sector + '\n' : ''}Scan: ${it.scan_name || 'Trigger'}${volTag}`;

    const payload = JSON.stringify({
      title,
      body,
      stock: it.stock,
      price: it.trigger_price,
      scan: it.scan_name,
      isHighVol,
      tvUrl: `https://in.tradingview.com/chart/?symbol=NSE:${encodeURIComponent(it.stock)}`
    });

    for (const [endpoint, sub] of pushSubscriptions.entries()) {
      webpush.sendNotification(sub, payload).catch((err) => {
        if (err.statusCode === 404 || err.statusCode === 410) {
          pushSubscriptions.delete(endpoint);
          db.query(`DELETE FROM push_subscriptions WHERE endpoint = $1`, [endpoint]).catch(() => {});
        } else {
          console.warn(`[push] error sending to ${endpoint.slice(0, 30)}...:`, err.message);
        }
      });
    }
  }
}

// --- Market Breadth Helper ---
async function updateBreadth(ratioVal, source = "manual", thresholdVal = null) {
  const ratio = parseFloat(ratioVal);
  if (isNaN(ratio) || !isFinite(ratio)) {
    throw new Error("Invalid ratio value: must be a finite number");
  }

  const threshold = thresholdVal != null && !isNaN(parseFloat(thresholdVal))
    ? parseFloat(thresholdVal)
    : (breadthState.threshold || 350);

  const oldStatus = breadthState.status;
  const status = ratio > threshold ? "GO" : "NO_GO";
  const now = new Date();
  const timeIst = moment().tz(TZ).format("hh:mm A");

  breadthState.ratio = Math.round(ratio * 100) / 100;
  breadthState.status = status;
  breadthState.threshold = threshold;
  breadthState.recorded_at = now;
  breadthState.time_ist = timeIst;
  breadthState.source = source;
  breadthState.lastCheck = now;

  try {
    await db.query(
      `INSERT INTO market_breadth (ratio, status, threshold, source, recorded_at) VALUES ($1, $2, $3, $4, $5)`,
      [breadthState.ratio, status, threshold, source, now]
    );
  } catch (err) {
    console.error("[breadth] failed to persist:", err.message);
  }

  broadcastBreadth();

  // If status flipped, notify via Telegram and Web Push!
  if (oldStatus !== status) {
    const isGo = status === "GO";
    const banner = isGo
      ? "🟢 <b>MARKET BREADTH REGIME: TRADE ON (GO)</b>"
      : "🔴 <b>MARKET BREADTH REGIME: NO TRADING (NO-GO)</b>";
    const advice = isGo
      ? "✅ Intraday market breadth ratio is strong (&gt;350). Intraday long trades permitted."
      : "⛔ Intraday market breadth ratio is weak (≤350). Do NOT open new intraday positions!";

    const msg = `${banner}\n\n` +
      `📊 <b>Ratio (4.5R):</b> <code>${breadthState.ratio.toFixed(2)}</code>\n` +
      `🎯 <b>Threshold:</b> &gt; ${threshold}\n` +
      `⏱ <b>Time:</b> ${timeIst} IST\n` +
      `📡 <b>Source:</b> ${escapeHtml(source)}\n\n` +
      `${advice}`;

    sendTelegramRaw(msg).catch((err) => {
      console.warn("[telegram] failed to send breadth notice:", err.message);
    });

    if (webpush && pushSubscriptions.size) {
      const pushMsg = JSON.stringify({
        title: isGo ? "🟢 Breadth GO: Trading Allowed" : "🔴 Breadth NO-GO: Trading Blocked",
        body: `Ratio 4.5R is ${breadthState.ratio.toFixed(1)} (${isGo ? 'Permitted' : 'Do not open positions'}).`,
        stock: "BREADTH",
        price: breadthState.ratio,
      });
      for (const [endpoint, sub] of pushSubscriptions.entries()) {
        webpush.sendNotification(sub, pushMsg).catch(() => {});
      }
    }
  }

  return breadthState;
}

// Health check (also used by keep-alive pings)
app.get("/health", (req, res) =>
  res.json({
    status: "ok",
    uptime: Math.floor(process.uptime()),
    clients: wss.clients.size,
    pushSubscribers: pushSubscriptions.size,
    breadth: breadthState,
    telegramConfigured: Boolean(process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID),
  })
);

// --- Market Breadth API Endpoints ---
app.get("/api/breadth", async (req, res) => {
  try {
    const { rows } = await db.query(
      `SELECT id, ratio, status, threshold, source,
              to_char(recorded_at AT TIME ZONE '${TZ}', 'YYYY-MM-DD') AS date_ist,
              to_char(recorded_at AT TIME ZONE '${TZ}', 'HH12:MI AM') AS time_ist,
              EXTRACT(EPOCH FROM (recorded_at AT TIME ZONE '${TZ}'))*1000 AS ts_ist,
              recorded_at
         FROM market_breadth
        ORDER BY recorded_at DESC LIMIT 50`
    );
    res.json({
      status: "ok",
      current: breadthState,
      history: rows,
    });
  } catch (err) {
    console.error("[/api/breadth GET]", err.message);
    res.status(500).json({ error: err.message, current: breadthState, history: [] });
  }
});

app.post("/api/breadth", async (req, res) => {
  try {
    const { ratio, source, threshold } = req.body || {};
    if (ratio == null || isNaN(parseFloat(ratio))) {
      return res.status(400).json({ error: "Missing or invalid 'ratio' value. Must be a number." });
    }
    const result = await updateBreadth(ratio, source || "manual", threshold);
    res.json({ status: "ok", breadth: result });
  } catch (err) {
    console.error("[/api/breadth POST]", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Web Push VAPID public key endpoint
app.get("/vapid-public-key", (req, res) => {
  res.json({
    publicKey: currentVapidKeys.publicKey || null,
    configured: Boolean(webpush && currentVapidKeys.publicKey)
  });
});

// Mobile push subscribe endpoint
app.post("/subscribe", async (req, res) => {
  const sub = req.body?.subscription;
  if (!sub || !sub.endpoint) {
    return res.status(400).json({ error: "Invalid subscription object" });
  }

  try {
    pushSubscriptions.set(sub.endpoint, sub);
    await db.query(
      `INSERT INTO push_subscriptions (endpoint, subscription) VALUES ($1, $2)
       ON CONFLICT (endpoint) DO UPDATE SET subscription = EXCLUDED.subscription`,
      [sub.endpoint, JSON.stringify(sub)]
    );
    console.log(`[push] registered subscriber (total: ${pushSubscriptions.size})`);
    res.json({ status: "subscribed", total: pushSubscriptions.size });
  } catch (err) {
    console.error("[push] subscribe error:", err.message);
    res.status(500).json({ error: "Failed to save subscription" });
  }
});

// Mobile push unsubscribe endpoint
app.post("/unsubscribe", async (req, res) => {
  const endpoint = req.body?.endpoint;
  if (endpoint) {
    pushSubscriptions.delete(endpoint);
    await db.query(`DELETE FROM push_subscriptions WHERE endpoint = $1`, [endpoint]).catch(() => {});
    console.log(`[push] unregistered subscriber (total: ${pushSubscriptions.size})`);
  }
  res.json({ status: "unsubscribed" });
});

// Diagnostic test endpoint to test Telegram delivery from browser
app.get("/test-telegram", async (req, res) => {
  const token  = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) {
    return res.status(400).json({
      ok: false,
      error: "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in environment variables.",
      help: [
        "1. Create a bot in Telegram via @BotFather to get your token.",
        "2. Find your numeric chat ID via @userinfobot.",
        "3. CRITICAL: Open your new bot in Telegram and click 'Start' (send /start) so it has permission to message you!",
        "4. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Render Environment variables and redeploy."
      ]
    });
  }

  try {
    const sample = {
      stock: "NIFTY-TEST",
      trigger_price: 24500.50,
      count: 1,
      scan_name: "Test Scan Connection",
      time_ist: new Date().toLocaleTimeString("en-IN", { timeZone: TZ, hour: "2-digit", minute: "2-digit", hour12: true })
    };
    const text = formatTelegramMessage(sample);
    const result = await sendTelegramRaw(text);
    res.json({
      ok: true,
      message: "Test message sent to Telegram successfully! Check your Telegram app.",
      telegramResponse: result
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
      troubleshooting: [
        "If 401 Unauthorized: The TELEGRAM_BOT_TOKEN is invalid or mistyped.",
        "If 400 Chat not found: The TELEGRAM_CHAT_ID is invalid.",
        "If 403 Forbidden: You must open the bot in Telegram and click 'Start' (/start) first before it can send you messages!"
      ],
      details: err.details || null
    });
  }
});

// Chartink webhook
app.post("/new-stocks", verifySecret, async (req, res) => {
  try {
    const items = transform(req.body);
    if (!items.length) return res.json({ status: "ok", processed: 0 });

    const deltaRows = await dbUtils.upsertAndFetchIST(items);
    await refreshCache(false, deltaRows);
    notifyTelegram(deltaRows);
    notifyPush(deltaRows);

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

// Smart Chartink Market Breadth Poller (Dashboard 508179 - 4.5R Ratio)
// Only polls during active trading hours (09:15–15:30 IST, Mon–Fri) AND when active WebSocket clients are connected.
// Outside market hours or when zero users are connected, Render sleeps undisturbed.
async function pollChartinkBreadth() {
  if (!timeUtils.isMarketHoursIST()) {
    return; // Outside NSE trading hours -> sleep
  }
  if (wss.clients.size === 0) {
    return; // No connected clients -> sleep
  }

  try {
    const dashRes = await fetch("https://chartink.com/dashboard/508179", {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
      signal: AbortSignal.timeout(10000)
    });

    if (!dashRes.ok) return;
    const html = await dashRes.text();
    const csrfMatch = html.match(/<meta name="csrf-token" content="([^"]+)"/);
    if (!csrfMatch) return;
    const csrf = csrfMatch[1];

    let cookieHeader = "";
    if (typeof dashRes.headers.getSetCookie === "function") {
      cookieHeader = dashRes.headers.getSetCookie().map(c => c.split(";")[0]).join("; ");
    } else {
      const raw = dashRes.headers.get("set-cookie");
      cookieHeader = raw ? raw.split(",").map(c => c.split(";")[0].trim()).join("; ") : "";
    }

    const form = new URLSearchParams();
    form.set("scan_clause", "( {cash} 1 = 1 )");
    form.set("column_clause", '( ( groupcount( {cash} 1 where daily "close - 1 candle ago close / 1 candle ago close * 100" >= 4.5 ) * 100 ) / ( groupcount( {cash} 1 where daily "close - 1 candle ago close / 1 candle ago close * 100" <= -4.5 ) ) ) as \'4.5R\'');
    form.set("order_clause", "ORDER BY 1 desc");

    const postRes = await fetch("https://chartink.com/screener/process", {
      method: "POST",
      headers: {
        "Host": "chartink.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "X-CSRF-TOKEN": csrf,
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": cookieHeader,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://chartink.com/dashboard/508179",
        "Origin": "https://chartink.com",
      },
      body: form.toString(),
      signal: AbortSignal.timeout(12000)
    });

    if (!postRes.ok) return;
    const json = await postRes.json();
    if (Array.isArray(json?.data) && json.data.length > 0) {
      const row = json.data[0];
      const val = row["4.5R"] ?? row["4.5r"] ?? row["ratio"] ?? Object.values(row)[0];
      const parsedVal = parseFloat(val);
      if (!isNaN(parsedVal) && isFinite(parsedVal)) {
        await updateBreadth(parsedVal, "chartink-server-poller");
        console.log(`[breadth] poller updated ratio to ${parsedVal}`);
      }
    }
  } catch (err) {
    console.warn(`[breadth] poller notice: ${err.message}`);
  }
}

setInterval(pollChartinkBreadth, 2 * 60 * 1000);

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
    if (process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
      console.log(`[telegram] enabled — mobile push active for chat ${process.env.TELEGRAM_CHAT_ID}`);
    } else {
      console.log("[telegram] disabled — TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set");
    }
    server.listen(PORT, () =>
      console.log(`[server] listening on port ${PORT}`)
    );
  } catch (e) {
    console.error("[startup] fatal:", e);
    process.exit(1);
  }
}

main();
