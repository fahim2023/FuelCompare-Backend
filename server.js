const express = require("express");
const cors = require("cors");
const { MongoClient } = require("mongodb");
const cron = require("node-cron");
require("dotenv").config({ path: ".env.local" });

// ── Express setup ─────────────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

// ── GOV.UK API URLs ───────────────────────────────────────────────────────────
const TOKEN_URL =
  "https://www.fuel-finder.service.gov.uk/api/v1/oauth/generate_access_token";
const SITES_URL = "https://www.fuel-finder.service.gov.uk/api/v1/pfs";
const PRICES_URL =
  "https://www.fuel-finder.service.gov.uk/api/v1/pfs/fuel-prices";

// ── Retailer feeds ────────────────────────────────────────────────────────────
const RETAILERS = [
  { name: "Asda", url: "https://storelocator.asda.com/fuel_prices_data.json" },
  {
    name: "Sainsbury's",
    url: "https://api.sainsburys.co.uk/v1/exports/latest/fuel_prices_data.json",
  },
  { name: "Morrisons", url: "https://www.morrisons.com/fuel-prices/fuel.json" },
  {
    name: "Motor Fuel Group",
    url: "https://fuel.motorfuelgroup.com/fuel_prices_data.json",
  },
  { name: "JET", url: "https://jetlocal.co.uk/fuel_prices_data.json" },
  { name: "Esso", url: "https://fuelprices.esso.co.uk/latestdata.json" },
  { name: "Ascona", url: "https://fuelprices.asconagroup.co.uk/newfuel.json" },
  {
    name: "Rontec",
    url: "https://www.rontec-servicestations.co.uk/fuel-prices/data/fuel_prices_data.json",
  },
  {
    name: "EG Group",
    url: "https://applegreenstores.com/fuel-prices/data.json",
  },
  { name: "Moto", url: "https://moto-way.com/fuel-price/fuel_prices.json" },
  {
    name: "SGN",
    url: "https://www.sgnretail.uk/files/data/SGN_daily_fuel_prices.json",
  },
];

// ── Date formatter ────────────────────────────────────────────────────────────
function formatDate(d) {
  const day = String(d.getDate()).padStart(2, "0");
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const year = d.getFullYear();
  const hours = String(d.getHours()).padStart(2, "0");
  const mins = String(d.getMinutes()).padStart(2, "0");
  const secs = String(d.getSeconds()).padStart(2, "0");
  return `${day}.${month}.${year} ${hours}:${mins}:${secs}`;
}

// ── In-memory cache ───────────────────────────────────────────────────────────
let cachedToken = null;
let tokenExpiry = 0;
let cachedStations = null;
let stationsExpiry = 0;

// ── MongoDB ───────────────────────────────────────────────────────────────────
const MONGODB_URI = process.env.MONGODB_URI;
let db = null;

async function getDb() {
  if (db) return db;
  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  db = client.db("fuelscan");
  console.log("MongoDB connected");
  return db;
}

// ── GOV.UK token ──────────────────────────────────────────────────────────────
async function getToken() {
  if (cachedToken && Date.now() < tokenExpiry - 60_000) return cachedToken;
  const clientId = process.env.FUEL_CLIENT_ID;
  const clientSecret = process.env.FUEL_CLIENT_SECRET;
  if (!clientId || !clientSecret)
    throw new Error(
      "Missing FUEL_CLIENT_ID or FUEL_CLIENT_SECRET in .env.local",
    );

  const res = await fetch(TOKEN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
    },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret,
      scope: "fuelfinder.read",
    }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Auth failed ${res.status}: ${text}`);
  }
  const data = await res.json();
  const tokenData = data.data || data;
  cachedToken = tokenData.access_token;
  tokenExpiry = Date.now() + (tokenData.expires_in || 3600) * 1000;
  return cachedToken;
}

// ── Fetch GOV.UK stations ─────────────────────────────────────────────────────
async function fetchGovStations() {
  const token = await getToken();
  const allSites = [];
  for (let batch = 1; batch <= 40; batch++) {
    const res = await fetch(`${SITES_URL}?batch-number=${batch}`, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
    });
    if (res.status === 404) break;
    if (!res.ok) {
      console.warn(`Sites batch ${batch} failed: HTTP ${res.status}`);
      break;
    }
    const data = await res.json();
    const rows = Array.isArray(data) ? data : data.data || [];
    if (!rows.length) break;
    allSites.push(...rows);
    if (rows.length < 500) break;
  }

  const allPrices = [];
  for (let batch = 1; batch <= 40; batch++) {
    const res = await fetch(`${PRICES_URL}?batch-number=${batch}`, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
    });
    if (res.status === 404) break;
    if (!res.ok) {
      console.warn(`Prices batch ${batch} failed: HTTP ${res.status}`);
      break;
    }
    const data = await res.json();
    const rows = Array.isArray(data) ? data : data.data || [];
    if (!rows.length) break;
    allPrices.push(...rows);
    if (rows.length < 500) break;
  }

  const priceMap = {};
  for (const p of allPrices) {
    const prices = {};
    let priceLastUpdated = null;
    if (Array.isArray(p.fuel_prices)) {
      for (const fp of p.fuel_prices) {
        if (fp.fuel_type && fp.price != null) {
          const fuelType =
            fp.fuel_type.includes("_") ?
              fp.fuel_type.split("_")[0]
            : fp.fuel_type;
          if (fp.price >= 100 && fp.price <= 220) prices[fuelType] = fp.price;
        }
        if (
          fp.price_last_updated &&
          (!priceLastUpdated || fp.price_last_updated > priceLastUpdated)
        )
          priceLastUpdated = fp.price_last_updated;
      }
    }
    priceMap[p.node_id] = { prices, priceLastUpdated };
  }

  return allSites
    .map((s) => {
      const priceData = priceMap[s.node_id] || {
        prices: {},
        priceLastUpdated: null,
      };
      return {
        id: s.node_id,
        brand: s.brand_name || s.trading_name || "Unknown",
        address: [s.location?.address_line_1, s.location?.address_line_2]
          .filter(Boolean)
          .join(", "),
        town: s.location?.city || "",
        county: s.location?.county || "",
        postcode: s.location?.postcode || "",
        lat: s.location?.latitude ?? null,
        lng: s.location?.longitude ?? null,
        prices: priceData.prices,
        priceLastUpdated: priceData.priceLastUpdated,
        source: "gov",
      };
    })
    .filter((s) => s.lat != null && s.lng != null);
}

// ── Fetch retailer feeds ──────────────────────────────────────────────────────
async function fetchRetailer(retailer) {
  const res = await fetch(retailer.url, {
    headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json" },
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  const feedLastUpdated = data.last_updated || null;
  const raw = data.stations || data.S || data.data || [];
  return raw
    .map((s) => ({
      id: s.site_id || s.id || Math.random().toString(36).slice(2),
      brand: s.brand || retailer.name,
      address: s.address || s.Address || "",
      postcode: s.postcode || s.Postcode || "",
      lat: s.location?.latitude ?? s.Latitude ?? s.lat ?? null,
      lng: s.location?.longitude ?? s.Longitude ?? s.lng ?? null,
      priceLastUpdated: s.last_updated || feedLastUpdated || null,
      prices: s.prices || {},
      source: "retailer",
    }))
    .filter((s) => s.lat != null && s.lng != null);
}

async function fetchRetailerStations() {
  const results = await Promise.allSettled(
    RETAILERS.map((r) => fetchRetailer(r)),
  );
  const stations = [];
  for (let i = 0; i < results.length; i++) {
    if (results[i].status === "fulfilled") stations.push(...results[i].value);
    else
      console.warn(
        `${RETAILERS[i].name} failed: ${results[i].reason?.message}`,
      );
  }
  return stations;
}

// ── Save daily snapshot ───────────────────────────────────────────────────────
async function saveDailySnapshot() {
  try {
    const database = await getDb();
    const today = new Date().toISOString().slice(0, 10);
    const nowFormatted = formatDate(new Date());

    const snapshotCol = database.collection("fuel_price_snapshots");
    const existing = await snapshotCol.findOne({ date: today });
    if (existing) {
      console.log(`[snapshot] Already saved for ${today}, skipping`);
      return { skipped: true, date: today };
    }

    console.log("[snapshot] Fetching station data...");
    const govStations = await fetchGovStations().catch((err) => {
      console.warn("[snapshot] GOV.UK failed:", err.message);
      return [];
    });
    const retailerStations = await fetchRetailerStations();

    const govPostcodes = new Set(
      govStations.map((s) => s.postcode?.trim().toUpperCase()).filter(Boolean),
    );
    const uniqueRetailer = retailerStations.filter((s) => {
      const pc = s.postcode?.trim().toUpperCase();
      return !pc || !govPostcodes.has(pc);
    });
    const allStations = [...govStations, ...uniqueRetailer];

    if (allStations.length === 0)
      throw new Error("No stations fetched — aborting snapshot");
    console.log(`[snapshot] ${allStations.length} stations fetched`);

    // 1. Upsert static station info
    const stationsCol = database.collection("stations");
    const todaySeenIds = allStations.map((s) => s.id);
    const stationOps = allStations.map((s) => ({
      updateOne: {
        filter: { stationId: s.id },
        update: {
          $set: {
            stationId: s.id,
            brand: s.brand,
            address: s.address,
            town: s.town || "",
            county: s.county || "",
            postcode: s.postcode,
            lat: s.lat,
            lng: s.lng,
            source: s.source,
            active: true,
            lastSeen: nowFormatted,
          },
        },
        upsert: true,
      },
    }));

    for (let i = 0; i < stationOps.length; i += 1000)
      await stationsCol.bulkWrite(stationOps.slice(i, i + 1000));

    // Mark stations not seen today as inactive
    const inactive = await stationsCol.updateMany(
      { stationId: { $nin: todaySeenIds }, active: { $ne: false } },
      { $set: { active: false, deactivatedAt: nowFormatted } },
    );
    if (inactive.modifiedCount > 0)
      console.log(
        `[snapshot] Marked ${inactive.modifiedCount} stations as inactive`,
      );

    // 2. Insert price history records
    const priceCol = database.collection("price_history");
    const VALID_FUEL_TYPES = ["E10", "B7", "E5", "SDV5"];
    const priceRecords = [];

    for (const s of allStations) {
      for (const [fuelType, price] of Object.entries(s.prices || {})) {
        if (!VALID_FUEL_TYPES.includes(fuelType)) continue;
        if (price == null || price < 100 || price > 200) continue;
        priceRecords.push({
          stationId: s.id,
          brand: s.brand,
          county: s.county || "",
          fuelType,
          price,
          snapshotDate: today,
          dayOfWeek: new Date().getDay(), // 0=Sun, 1=Mon ... 6=Sat
          created_at: nowFormatted,
        });
      }
    }

    for (let i = 0; i < priceRecords.length; i += 1000)
      await priceCol.insertMany(priceRecords.slice(i, i + 1000));
    console.log(`[snapshot] Inserted ${priceRecords.length} price records`);

    // 3. Save national averages snapshot
    const buckets = { E10: [], B7: [], E5: [], SDV5: [] };
    for (const r of priceRecords) {
      if (buckets[r.fuelType]) buckets[r.fuelType].push(r.price);
    }

    const avg = (arr) =>
      arr.length ?
        Math.round((arr.reduce((a, b) => a + b, 0) / arr.length) * 10) / 10
      : null;

    const snapshot = {
      date: today,
      avg_e10: avg(buckets.E10),
      avg_b7: avg(buckets.B7),
      avg_e5: avg(buckets.E5),
      avg_sdv5: avg(buckets.SDV5),
      min_e10:
        buckets.E10.length ?
          Math.round(Math.min(...buckets.E10) * 10) / 10
        : null,
      max_e10:
        buckets.E10.length ?
          Math.round(Math.max(...buckets.E10) * 10) / 10
        : null,
      min_b7:
        buckets.B7.length ?
          Math.round(Math.min(...buckets.B7) * 10) / 10
        : null,
      max_b7:
        buckets.B7.length ?
          Math.round(Math.max(...buckets.B7) * 10) / 10
        : null,
      station_count: allStations.length,
      price_records: priceRecords.length,
      source: "gov",
      created_at: nowFormatted,
    };

    await snapshotCol.insertOne(snapshot);
    console.log(
      `[snapshot] Done: E10=${snapshot.avg_e10}p, B7=${snapshot.avg_b7}p, records=${priceRecords.length}`,
    );
    return { success: true, snapshot };
  } catch (err) {
    console.error("[snapshot] Error:", err.message);
    throw err;
  }
}

// ── Routes ────────────────────────────────────────────────────────────────────

// GET /fuel-stations
app.get("/fuel-stations", async (req, res) => {
  try {
    if (cachedStations && Date.now() < stationsExpiry)
      return res.json(cachedStations);

    let govStations = [];
    try {
      govStations = await fetchGovStations();
    } catch (err) {
      console.warn(`GOV.UK failed: ${err.message}`);
    }

    const retailerStations = await fetchRetailerStations();
    const govPostcodes = new Set(
      govStations.map((s) => s.postcode?.trim().toUpperCase()).filter(Boolean),
    );
    const uniqueRetailer = retailerStations.filter((s) => {
      const pc = s.postcode?.trim().toUpperCase();
      return !pc || !govPostcodes.has(pc);
    });

    const stations = [...govStations, ...uniqueRetailer];
    const response = { stations, count: stations.length };
    cachedStations = response;
    stationsExpiry = Date.now() + 10 * 60 * 1000;
    res.json(response);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /station/:id
app.get("/station/:id", async (req, res) => {
  try {
    const database = await getDb();
    const station = await database
      .collection("stations")
      .findOne({ stationId: req.params.id });
    if (!station) return res.status(404).json({ error: "Station not found" });

    const today = new Date().toISOString().slice(0, 10);
    const priceRecords = await database
      .collection("price_history")
      .find({ stationId: req.params.id, snapshotDate: today })
      .toArray();

    const prices = {};
    priceRecords.forEach((r) => {
      prices[r.fuelType] = r.price;
    });

    res.json({ ...station, prices, fuels: prices });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /price-history — national daily averages for trends chart
app.get("/price-history", async (req, res) => {
  try {
    const database = await getDb();
    const days = parseInt(req.query.days) || 90;
    const since = new Date();
    since.setDate(since.getDate() - days);
    const sinceStr = since.toISOString().slice(0, 10);

    const snapshots = await database
      .collection("fuel_price_snapshots")
      .find({ date: { $gte: sinceStr } })
      .sort({ date: 1 })
      .toArray();

    res.json({ snapshots, count: snapshots.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /station-history/:stationId — per-station price history
app.get("/station-history/:stationId", async (req, res) => {
  try {
    const database = await getDb();
    const days = parseInt(req.query.days) || 90;
    const since = new Date();
    since.setDate(since.getDate() - days);
    const sinceStr = since.toISOString().slice(0, 10);

    const records = await database
      .collection("price_history")
      .find({
        stationId: req.params.stationId,
        snapshotDate: { $gte: sinceStr },
      })
      .sort({ snapshotDate: 1 })
      .toArray();

    const byDate = {};
    for (const r of records) {
      if (!byDate[r.snapshotDate])
        byDate[r.snapshotDate] = { date: r.snapshotDate };
      byDate[r.snapshotDate][r.fuelType] = r.price;
    }

    res.json({
      history: Object.values(byDate).sort((a, b) =>
        a.date.localeCompare(b.date),
      ),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /price-stats — week on week, cheapest day, price velocity
app.get("/price-stats", async (req, res) => {
  try {
    const database = await getDb();
    const fuelType = req.query.fuelType || "E10";
    const dbFuelField = `avg_${fuelType.toLowerCase()}`;

    // Get last 14 days of snapshots
    const since14 = new Date();
    since14.setDate(since14.getDate() - 14);
    const snapshots = await database
      .collection("fuel_price_snapshots")
      .find({ date: { $gte: since14.toISOString().slice(0, 10) } })
      .sort({ date: 1 })
      .toArray();

    const thisWeek = snapshots.slice(-7);
    const lastWeek = snapshots.slice(-14, -7);

    const avg = (arr, field) => {
      const vals = arr.map((s) => s[field]).filter((v) => v != null);
      return vals.length ?
          Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 10) / 10
        : null;
    };

    const thisWeekAvg = avg(thisWeek, dbFuelField);
    const lastWeekAvg = avg(lastWeek, dbFuelField);
    const weekOnWeek =
      thisWeekAvg && lastWeekAvg ?
        Math.round((thisWeekAvg - lastWeekAvg) * 10) / 10
      : null;

    // Price velocity — difference between last 3 days
    const last3 = snapshots
      .slice(-3)
      .map((s) => s[dbFuelField])
      .filter((v) => v != null);
    const velocity =
      last3.length >= 2 ?
        Math.round((last3[last3.length - 1] - last3[0]) * 10) / 10
      : null;

    // Cheapest day of week from price_history
    const since90 = new Date();
    since90.setDate(since90.getDate() - 90);
    const dayAggs = await database
      .collection("price_history")
      .aggregate([
        {
          $match: {
            fuelType,
            snapshotDate: { $gte: since90.toISOString().slice(0, 10) },
          },
        },
        {
          $group: {
            _id: "$dayOfWeek",
            avgPrice: { $avg: "$price" },
            count: { $sum: 1 },
          },
        },
        { $sort: { avgPrice: 1 } },
      ])
      .toArray();

    const DAY_NAMES = [
      "Sunday",
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
    ];
    const cheapestDay =
      dayAggs.length ?
        {
          day: DAY_NAMES[dayAggs[0]._id],
          avgPrice: Math.round(dayAggs[0].avgPrice * 10) / 10,
        }
      : null;
    const mostExpensiveDay =
      dayAggs.length ?
        {
          day: DAY_NAMES[dayAggs[dayAggs.length - 1]._id],
          avgPrice: Math.round(dayAggs[dayAggs.length - 1].avgPrice * 10) / 10,
        }
      : null;

    const allDays = dayAggs
      .map((d) => ({
        day: DAY_NAMES[d._id],
        dayIndex: d._id,
        avgPrice: Math.round(d.avgPrice * 10) / 10,
        count: d.count,
      }))
      .sort((a, b) => a.dayIndex - b.dayIndex);

    res.json({
      fuelType,
      thisWeekAvg,
      lastWeekAvg,
      weekOnWeek,
      velocity,
      cheapestDay,
      mostExpensiveDay,
      allDays,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /brand-averages — average price per brand
app.get("/brand-averages", async (req, res) => {
  try {
    const database = await getDb();
    const fuelType = req.query.fuelType || "E10";
    const days = parseInt(req.query.days) || 7;
    const since = new Date();
    since.setDate(since.getDate() - days);
    const sinceStr = since.toISOString().slice(0, 10);

    const results = await database
      .collection("price_history")
      .aggregate([
        { $match: { fuelType, snapshotDate: { $gte: sinceStr } } },
        {
          $group: {
            _id: "$brand",
            avgPrice: { $avg: "$price" },
            minPrice: { $min: "$price" },
            maxPrice: { $max: "$price" },
            count: { $sum: 1 },
          },
        },
        { $match: { count: { $gte: 3 } } }, // only brands with enough data
        { $sort: { avgPrice: 1 } },
        { $limit: 20 },
      ])
      .toArray();

    const brands = results.map((r) => ({
      brand: r._id,
      avgPrice: Math.round(r.avgPrice * 10) / 10,
      minPrice: Math.round(r.minPrice * 10) / 10,
      maxPrice: Math.round(r.maxPrice * 10) / 10,
      count: r.count,
    }));

    res.json({ brands, fuelType, days });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /regional-averages — average price per county/region
app.get("/regional-averages", async (req, res) => {
  try {
    const database = await getDb();
    const fuelType = req.query.fuelType || "E10";
    const days = parseInt(req.query.days) || 7;
    const since = new Date();
    since.setDate(since.getDate() - days);
    const sinceStr = since.toISOString().slice(0, 10);

    const results = await database
      .collection("price_history")
      .aggregate([
        {
          $match: {
            fuelType,
            snapshotDate: { $gte: sinceStr },
            county: { $ne: "" },
          },
        },
        {
          $group: {
            _id: "$county",
            avgPrice: { $avg: "$price" },
            minPrice: { $min: "$price" },
            maxPrice: { $max: "$price" },
            count: { $sum: 1 },
          },
        },
        { $match: { count: { $gte: 5 } } },
        { $sort: { avgPrice: 1 } },
      ])
      .toArray();

    const regions = results.map((r) => ({
      county: r._id,
      avgPrice: Math.round(r.avgPrice * 10) / 10,
      minPrice: Math.round(r.minPrice * 10) / 10,
      maxPrice: Math.round(r.maxPrice * 10) / 10,
      count: r.count,
    }));

    res.json({ regions, fuelType, days });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /cheapest — cheapest stations for a given fuel type and date
app.get("/cheapest", async (req, res) => {
  try {
    const database = await getDb();
    const fuelType = req.query.fuelType || "E10";
    const date = req.query.date || new Date().toISOString().slice(0, 10);
    const limit = parseInt(req.query.limit) || 10;

    const records = await database
      .collection("price_history")
      .find({ fuelType, snapshotDate: date })
      .sort({ price: 1 })
      .limit(limit)
      .toArray();

    const stationIds = records.map((r) => r.stationId);
    const stations = await database
      .collection("stations")
      .find({ stationId: { $in: stationIds }, active: true })
      .toArray();

    const stationMap = Object.fromEntries(
      stations.map((s) => [s.stationId, s]),
    );
    const results = records.map((r) => ({
      ...stationMap[r.stationId],
      price: r.price,
      fuelType: r.fuelType,
      date: r.snapshotDate,
    }));

    res.json({ results, count: results.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /save-snapshot — manually trigger
app.post("/save-snapshot", async (req, res) => {
  try {
    const result = await saveDailySnapshot();
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /clear-cache
app.get("/clear-cache", (req, res) => {
  cachedStations = null;
  stationsExpiry = 0;
  cachedToken = null;
  tokenExpiry = 0;
  res.json({ ok: true, message: "Cache cleared" });
});

// GET /health
app.get("/health", (req, res) => res.json({ ok: true }));

// ── Cron: daily at 6am ────────────────────────────────────────────────────────
cron.schedule("0 6 * * *", async () => {
  console.log("[cron] Running daily snapshot...");
  await saveDailySnapshot().catch((err) =>
    console.error("[cron] Snapshot failed:", err.message),
  );
});
console.log("Daily snapshot cron scheduled at 6am");

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`UK Fuel Proxy running on port ${PORT}`));
