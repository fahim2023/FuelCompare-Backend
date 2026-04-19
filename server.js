const express = require("express");
const cors = require("cors");
const { MongoClient } = require("mongodb");
const cron = require("node-cron");
require("dotenv").config({ path: ".env.local" });

const app = express();
app.use(cors());
app.use(express.json());

const TOKEN_URL =
  "https://www.fuel-finder.service.gov.uk/api/v1/oauth/generate_access_token";
const SITES_URL = "https://www.fuel-finder.service.gov.uk/api/v1/pfs";
const PRICES_URL =
  "https://www.fuel-finder.service.gov.uk/api/v1/pfs/fuel-prices";

// const RETAILERS = [
//   { name: "Asda", url: "https://storelocator.asda.com/fuel_prices_data.json" },
//   {
//     name: "Sainsbury's",
//     url: "https://api.sainsburys.co.uk/v1/exports/latest/fuel_prices_data.json",
//   },
//   { name: "Morrisons", url: "https://www.morrisons.com/fuel-prices/fuel.json" },
//   {
//     name: "Motor Fuel Group",
//     url: "https://fuel.motorfuelgroup.com/fuel_prices_data.json",
//   },
//   { name: "JET", url: "https://jetlocal.co.uk/fuel_prices_data.json" },
//   { name: "Esso", url: "https://fuelprices.esso.co.uk/latestdata.json" },
//   { name: "Ascona", url: "https://fuelprices.asconagroup.co.uk/newfuel.json" },
//   {
//     name: "Rontec",
//     url: "https://www.rontec-servicestations.co.uk/fuel-prices/data/fuel_prices_data.json",
//   },
//   {
//     name: "EG Group",
//     url: "https://applegreenstores.com/fuel-prices/data.json",
//   },
//   { name: "Moto", url: "https://moto-way.com/fuel-price/fuel_prices.json" },
//   {
//     name: "SGN",
//     url: "https://www.sgnretail.uk/files/data/SGN_daily_fuel_prices.json",
//   },
// ];

function formatDate(d) {
  const day = String(d.getDate()).padStart(2, "0");
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const year = d.getFullYear();
  const hours = String(d.getHours()).padStart(2, "0");
  const mins = String(d.getMinutes()).padStart(2, "0");
  const secs = String(d.getSeconds()).padStart(2, "0");
  return `${day}.${month}.${year} ${hours}:${mins}:${secs}`;
}

// ── Haversine distance (miles) ────────────────────────────────────────────────
function haversineMiles(lat1, lon1, lat2, lon2) {
  const R = 3958.8;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

let cachedToken = null;
let tokenExpiry = 0;

// Track if a live price update is already running to prevent overlaps
let liveUpdateRunning = false;

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
      const openingHours =
        s.opening_times?.usual_days ?
          {
            monday: s.opening_times.usual_days.monday || null,
            tuesday: s.opening_times.usual_days.tuesday || null,
            wednesday: s.opening_times.usual_days.wednesday || null,
            thursday: s.opening_times.usual_days.thursday || null,
            friday: s.opening_times.usual_days.friday || null,
            saturday: s.opening_times.usual_days.saturday || null,
            sunday: s.opening_times.usual_days.sunday || null,
          }
        : null;
      const is24Hours =
        openingHours ?
          Object.values(openingHours).some((d) => d?.is_24_hours === true)
        : false;
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
        phone: s.public_phone_number || null,
        isMotorway: s.is_motorway_service_station || false,
        isSupermarket: s.is_supermarket_service_station || false,
        is24Hours,
        openingHours,
        source: "gov",
      };
    })
    .filter((s) => s.lat != null && s.lng != null);
}

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

// ── Update live prices in MongoDB (runs every 10 mins) ────────────────────────
async function updateLivePrices() {
  if (liveUpdateRunning) {
    console.log("[live] Update already running, skipping");
    return;
  }
  liveUpdateRunning = true;
  try {
    console.log("[live] Fetching fresh station data...");
    const govStations = await fetchGovStations().catch((err) => {
      console.warn("[live] GOV.UK failed:", err.message);
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

    if (allStations.length === 0) {
      console.warn("[live] No stations fetched, skipping update");
      return;
    }

    const database = await getDb();
    const liveCol = database.collection("live_prices");
    const stationsCol = database.collection("stations");
    const nowFormatted = formatDate(new Date());

    // Mark retailer duplicate stations as inactive if GOV.UK has the same postcode
    // This fixes cases like Applegreen vs EG On The Move at the same location
    const govPostcodeSet = new Set(
      govStations.map((s) => s.postcode?.trim().toUpperCase()).filter(Boolean),
    );
    const retailerDuplicateIds = retailerStations
      .filter((s) => {
        const pc = s.postcode?.trim().toUpperCase();
        return pc && govPostcodeSet.has(pc);
      })
      .map((s) => s.id);

    if (retailerDuplicateIds.length > 0) {
      await stationsCol.updateMany(
        { stationId: { $in: retailerDuplicateIds } },
        { $set: { active: false, deactivatedAt: nowFormatted } },
      );
      await liveCol.deleteMany({ stationId: { $in: retailerDuplicateIds } });
      console.log(
        `[live] Deactivated ${retailerDuplicateIds.length} retailer duplicates superseded by GOV.UK data`,
      );
    }

    // Upsert live prices — one doc per station, overwrites every 10 mins
    const livePriceOps = allStations.map((s) => ({
      updateOne: {
        filter: { stationId: s.id },
        update: {
          $set: {
            stationId: s.id,
            brand: s.brand,
            prices: s.prices || {},
            priceLastUpdated: s.priceLastUpdated || null,
            source: s.source,
            lastRefreshed: nowFormatted,
          },
        },
        upsert: true,
      },
    }));

    for (let i = 0; i < livePriceOps.length; i += 1000)
      await liveCol.bulkWrite(livePriceOps.slice(i, i + 1000));

    // Also upsert station static info
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
            phone: s.phone || null,
            isMotorway: s.isMotorway || false,
            isSupermarket: s.isSupermarket || false,
            is24Hours: s.is24Hours || false,
            openingHours: s.openingHours || null,
          },
        },
        upsert: true,
      },
    }));

    for (let i = 0; i < stationOps.length; i += 1000)
      await stationsCol.bulkWrite(stationOps.slice(i, i + 1000));

    console.log(`[live] Updated ${allStations.length} stations in MongoDB`);
  } catch (err) {
    console.error("[live] Update failed:", err.message);
  } finally {
    liveUpdateRunning = false;
  }
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
            phone: s.phone || null,
            isMotorway: s.isMotorway || false,
            isSupermarket: s.isSupermarket || false,
            is24Hours: s.is24Hours || false,
            openingHours: s.openingHours || null,
          },
        },
        upsert: true,
      },
    }));

    for (let i = 0; i < stationOps.length; i += 1000)
      await stationsCol.bulkWrite(stationOps.slice(i, i + 1000));

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
    const VALID_FUEL_TYPES = ["E10", "B7", "E5", "B10", "HVO"];
    const priceRecords = [];

    for (const s of allStations) {
      for (const [fuelType, price] of Object.entries(s.prices || {})) {
        if (!VALID_FUEL_TYPES.includes(fuelType)) continue;
        if (price == null || price < 100 || price > 220) continue;
        priceRecords.push({
          stationId: s.id,
          brand: s.brand,
          county: s.county || "",
          fuelType,
          price,
          snapshotDate: today,
          dayOfWeek: new Date().getDay(),
          created_at: nowFormatted,
        });
      }
    }

    for (let i = 0; i < priceRecords.length; i += 1000)
      await priceCol.insertMany(priceRecords.slice(i, i + 1000));
    console.log(`[snapshot] Inserted ${priceRecords.length} price records`);

    // 3. Save national averages snapshot
    const buckets = { E10: [], B7: [], E5: [], B10: [], HVO: [] };
    for (const r of priceRecords) {
      if (buckets[r.fuelType]) buckets[r.fuelType].push(r.price);
    }

    const avg = (arr) =>
      arr.length ?
        Math.round((arr.reduce((a, b) => a + b, 0) / arr.length) * 10) / 10
      : null;
    const minOf = (arr) =>
      arr.length ? Math.round(Math.min(...arr) * 10) / 10 : null;
    const maxOf = (arr) =>
      arr.length ? Math.round(Math.max(...arr) * 10) / 10 : null;

    const snapshot = {
      date: today,
      avg_e10: avg(buckets.E10),
      avg_b7: avg(buckets.B7),
      avg_e5: avg(buckets.E5),
      avg_b10: avg(buckets.B10),
      avg_hvo: avg(buckets.HVO),
      min_e10: minOf(buckets.E10),
      max_e10: maxOf(buckets.E10),
      min_b7: minOf(buckets.B7),
      max_b7: maxOf(buckets.B7),
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

// GET /fuel-stations/nearby — fast filtered endpoint served from MongoDB
app.get("/fuel-stations/nearby", async (req, res) => {
  try {
    const lat = parseFloat(req.query.lat);
    const lng = parseFloat(req.query.lng);
    const radius = parseFloat(req.query.radius) || 5;

    if (isNaN(lat) || isNaN(lng))
      return res.status(400).json({ error: "lat and lng are required" });

    const database = await getDb();
    const liveCol = database.collection("live_prices");
    const stCol = database.collection("stations");

    const liveCount = await liveCol.countDocuments();

    if (liveCount === 0) {
      // No live data yet — fall back to full fetch
      console.log("[nearby] No live_prices data, falling back to API");
      let govStations = [];
      try {
        govStations = await fetchGovStations();
      } catch (e) {
        console.warn(e.message);
      }
      const retailerStations = await fetchRetailerStations();
      const govPostcodes = new Set(
        govStations
          .map((s) => s.postcode?.trim().toUpperCase())
          .filter(Boolean),
      );
      const uniqueRetailer = retailerStations.filter((s) => {
        const pc = s.postcode?.trim().toUpperCase();
        return !pc || !govPostcodes.has(pc);
      });
      const all = [...govStations, ...uniqueRetailer];
      const nearby = all
        .filter((s) => s.lat && s.lng)
        .map((s) => ({
          ...s,
          distance: haversineMiles(lat, lng, s.lat, s.lng),
        }))
        .filter((s) => s.distance <= radius + 2);
      return res.json({ stations: nearby, count: nearby.length });
    }

    // Add a generous buffer to account for radius
    const buffer = (radius + 2) * 0.0145;
    const lngFactor = Math.cos((lat * Math.PI) / 180);

    // Pre-filter by bounding box in MongoDB for speed
    const [stationDocs, liveDocs] = await Promise.all([
      stCol
        .find({
          active: true,
          lat: { $gte: lat - buffer, $lte: lat + buffer },
          lng: {
            $gte: lng - buffer / lngFactor,
            $lte: lng + buffer / lngFactor,
          },
        })
        .toArray(),
      liveCol.find({}).toArray(),
    ]);

    const priceMap = {};
    for (const l of liveDocs) {
      priceMap[l.stationId] = {
        prices: l.prices || {},
        priceLastUpdated: l.priceLastUpdated || null,
        lastRefreshed: l.lastRefreshed || null,
      };
    }

    const stations = stationDocs
      .map((s) => {
        const live = priceMap[s.stationId] || {};
        const distance = haversineMiles(lat, lng, s.lat, s.lng);
        return {
          id: s.stationId,
          brand: s.brand,
          address: s.address,
          town: s.town || "",
          county: s.county || "",
          postcode: s.postcode || "",
          lat: s.lat,
          lng: s.lng,
          prices: live.prices || {},
          priceLastUpdated: live.priceLastUpdated || null,
          lastRefreshed: live.lastRefreshed || null,
          phone: s.phone || null,
          isMotorway: s.isMotorway || false,
          isSupermarket: s.isSupermarket || false,
          is24Hours: s.is24Hours || false,
          openingHours: s.openingHours || null,
          source: s.source,
          distance,
        };
      })
      .filter((s) => s.lat != null && s.lng != null && s.distance <= radius + 2)
      .sort((a, b) => a.distance - b.distance);

    res.json({ stations, count: stations.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /fuel-stations — now served from MongoDB live_prices + stations
app.get("/fuel-stations", async (req, res) => {
  try {
    const database = await getDb();
    const liveCol = database.collection("live_prices");
    const stCol = database.collection("stations");

    // Check if we have live price data
    const liveCount = await liveCol.countDocuments();
    if (liveCount === 0) {
      // No live data yet — fall back to live API fetch
      console.log("[fuel-stations] No live_prices data, falling back to API");
      let govStations = [];
      try {
        govStations = await fetchGovStations();
      } catch (e) {
        console.warn(e.message);
      }
      const retailerStations = await fetchRetailerStations();
      const govPostcodes = new Set(
        govStations
          .map((s) => s.postcode?.trim().toUpperCase())
          .filter(Boolean),
      );
      const uniqueRetailer = retailerStations.filter((s) => {
        const pc = s.postcode?.trim().toUpperCase();
        return !pc || !govPostcodes.has(pc);
      });
      const stations = [...govStations, ...uniqueRetailer];
      return res.json({ stations, count: stations.length });
    }

    // Serve from MongoDB — fast!
    const [stationDocs, liveDocs] = await Promise.all([
      stCol.find({ active: true }).toArray(),
      liveCol.find({}).toArray(),
    ]);

    // Build a price map from live_prices
    const priceMap = {};
    for (const l of liveDocs) {
      priceMap[l.stationId] = {
        prices: l.prices || {},
        priceLastUpdated: l.priceLastUpdated || null,
        lastRefreshed: l.lastRefreshed || null,
      };
    }

    // Merge station info with live prices
    const stations = stationDocs
      .map((s) => {
        const live = priceMap[s.stationId] || {};
        return {
          id: s.stationId,
          brand: s.brand,
          address: s.address,
          town: s.town || "",
          county: s.county || "",
          postcode: s.postcode || "",
          lat: s.lat,
          lng: s.lng,
          prices: live.prices || {},
          priceLastUpdated: live.priceLastUpdated || null,
          lastRefreshed: live.lastRefreshed || null,
          phone: s.phone || null,
          isMotorway: s.isMotorway || false,
          isSupermarket: s.isSupermarket || false,
          is24Hours: s.is24Hours || false,
          openingHours: s.openingHours || null,
          source: s.source,
        };
      })
      .filter((s) => s.lat != null && s.lng != null);

    res.json({ stations, count: stations.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /station/:id — serve from live_prices for freshest data
app.get("/station/:id", async (req, res) => {
  try {
    const database = await getDb();
    const station = await database
      .collection("stations")
      .findOne({ stationId: req.params.id });
    if (!station) return res.status(404).json({ error: "Station not found" });

    // Try live_prices first, fall back to today's price_history
    const live = await database
      .collection("live_prices")
      .findOne({ stationId: req.params.id });
    let prices = live?.prices || {};

    if (!Object.keys(prices).length) {
      const today = new Date().toISOString().slice(0, 10);
      const priceRecords = await database
        .collection("price_history")
        .find({ stationId: req.params.id, snapshotDate: today })
        .toArray();
      priceRecords.forEach((r) => {
        prices[r.fuelType] = r.price;
      });
    }

    res.json({ ...station, prices, fuels: prices });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/price-history", async (req, res) => {
  try {
    const database = await getDb();
    const days = parseInt(req.query.days) || 90;
    const since = new Date();
    since.setDate(since.getDate() - days);
    const snapshots = await database
      .collection("fuel_price_snapshots")
      .find({ date: { $gte: since.toISOString().slice(0, 10) } })
      .sort({ date: 1 })
      .toArray();
    res.json({ snapshots, count: snapshots.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/station-history/:stationId", async (req, res) => {
  try {
    const database = await getDb();
    const days = parseInt(req.query.days) || 90;
    const since = new Date();
    since.setDate(since.getDate() - days);
    const records = await database
      .collection("price_history")
      .find({
        stationId: req.params.stationId,
        snapshotDate: { $gte: since.toISOString().slice(0, 10) },
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

app.get("/price-stats", async (req, res) => {
  try {
    const database = await getDb();
    const fuelType = req.query.fuelType || "E10";
    const dbFuelField = `avg_${fuelType.toLowerCase()}`;
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
    const last3 = snapshots
      .slice(-3)
      .map((s) => s[dbFuelField])
      .filter((v) => v != null);
    const velocity =
      last3.length >= 2 ?
        Math.round((last3[last3.length - 1] - last3[0]) * 10) / 10
      : null;
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

app.get("/brand-averages", async (req, res) => {
  try {
    const database = await getDb();
    const fuelType = req.query.fuelType || "E10";
    const days = parseInt(req.query.days) || 7;
    const since = new Date();
    since.setDate(since.getDate() - days);
    const results = await database
      .collection("price_history")
      .aggregate([
        {
          $match: {
            fuelType,
            snapshotDate: { $gte: since.toISOString().slice(0, 10) },
          },
        },
        {
          $group: {
            _id: "$brand",
            avgPrice: { $avg: "$price" },
            minPrice: { $min: "$price" },
            maxPrice: { $max: "$price" },
            count: { $sum: 1 },
          },
        },
        { $match: { count: { $gte: 3 } } },
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

app.get("/regional-averages", async (req, res) => {
  try {
    const database = await getDb();
    const fuelType = req.query.fuelType || "E10";
    const days = parseInt(req.query.days) || 7;
    const since = new Date();
    since.setDate(since.getDate() - days);
    const results = await database
      .collection("price_history")
      .aggregate([
        {
          $match: {
            fuelType,
            snapshotDate: { $gte: since.toISOString().slice(0, 10) },
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
        { $match: { count: { $gte: 5 }, _id: { $ne: "" } } },
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

app.post("/save-snapshot", async (req, res) => {
  try {
    const result = await saveDailySnapshot();
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /update-live — manually trigger live price update
app.post("/update-live", async (req, res) => {
  try {
    await updateLivePrices();
    res.json({ ok: true, message: "Live prices updated" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/clear-cache", (req, res) => {
  res.json({ ok: true, message: "Cache cleared (now served from MongoDB)" });
});

app.get("/health", (req, res) => res.json({ ok: true }));

// ── Cron: update live prices every 10 minutes ─────────────────────────────────
cron.schedule("*/10 * * * *", async () => {
  console.log("[cron] Updating live prices...");
  await updateLivePrices();
});
console.log("Live price update cron scheduled every 10 minutes");

// ── Cron: daily snapshot at 6am ───────────────────────────────────────────────
cron.schedule("0 6 * * *", async () => {
  console.log("[cron] Running daily snapshot...");
  await saveDailySnapshot().catch((err) =>
    console.error("[cron] Snapshot failed:", err.message),
  );
});
console.log("Daily snapshot cron scheduled at 6am");

// ── Trigger initial live price load on startup ────────────────────────────────
getDb().then(() => {
  console.log("[startup] Triggering initial live price load...");
  updateLivePrices();
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`UK Fuel Proxy running on port ${PORT}`));
