const express = require("express");
const cors = require("cors");
require("dotenv").config({ path: ".env.local" });

const app = express();

app.use(cors());
app.use(express.json());

const BASE_URL = "https://www.fuel-finder.service.gov.uk/api/v1";
const TOKEN_URL = `${BASE_URL}/oauth/generate_access_token`;
const PRICES_URL = `${BASE_URL}/prices`;
let cachedToken = null;
let tokenExpiry = 0;
let cachedStations = null;
let stationsExpiry = 0;

async function getToken() {
  if (cachedToken && Date.now() < tokenExpiry - 60000) {
    return cachedToken;
  }

  const clientId = process.env.FUEL_CLIENT_ID;
  const clientSecret = process.env.FUEL_CLIENT_SECRET;

  if (!clientId || !clientSecret) {
    throw new Error(
      "Missing FUEL_CLIENT_ID or FUEL_CLIENT_SECRET in .env.local",
    );
  }

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

  if (!tokenData.access_token) {
    throw new Error("No access token returned");
  }

  cachedToken = tokenData.access_token;
  tokenExpiry = Date.now() + (tokenData.expires_in || 3600) * 1000;
  return cachedToken;
}

async function fetchBatch(token, batchNumber) {
  const res = await fetch(`${PRICES_URL}?batch-number=${batchNumber}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
    },
  });

  if (res.status === 404) return null;

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status} on batch ${batchNumber}: ${text}`);
  }

  const data = await res.json();
  return data.data || data;
}

function normalize(station) {
  const prices = {};
  let priceLastUpdated = null;

  if (Array.isArray(station.fuel_prices)) {
    for (const fp of station.fuel_prices) {
      if (fp.fuel_type && fp.price != null) {
        prices[fp.fuel_type] = fp.price;
      }
      if (fp.price_last_updated) {
        if (!priceLastUpdated || fp.price_last_updated > priceLastUpdated) {
          priceLastUpdated = fp.price_last_updated;
        }
      }
    }
  }

  return {
    id: station.node_id || station.site_id || station.id,
    brand: station.brand || station.trading_name || "Unknown",
    address: [station.address, station.address_line_2]
      .filter(Boolean)
      .join(", "),
    town: station.town || "",
    county: station.county || "",
    postcode: station.postcode || "",
    lat: station.location?.latitude ?? station.latitude ?? null,
    lng: station.location?.longitude ?? station.longitude ?? null,
    prices,
    priceLastUpdated,
  };
}

app.get("/fuel-stations", async (req, res) => {
  try {
    if (cachedStations && Date.now() < stationsExpiry) {
      return res.json(cachedStations);
    }

    const token = await getToken();
    const batch1 = await fetchBatch(token, 1);

    if (!batch1) {
      return res.json({ stations: [], count: 0 });
    }

    let all = [...batch1];

    if (batch1.length >= 500) {
      const batchPromises = [];
      for (let i = 2; i <= 40; i++) {
        batchPromises.push(fetchBatch(token, i));
      }

      const batches = await Promise.all(batchPromises);

      for (const batch of batches) {
        if (!batch || batch.length === 0) break;
        all.push(...batch);
        if (batch.length < 500) break;
      }
    }

    const stations = all
      .map(normalize)
      .filter((s) => s.lat != null && s.lng != null);

    const response = {
      stations,
      count: stations.length,
    };

    cachedStations = response;
    stationsExpiry = Date.now() + 10 * 60 * 1000;

    res.json(response);
  } catch (err) {
    console.error("Proxy error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`UK Fuel Proxy running on port ${PORT}`);
});
