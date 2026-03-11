/**
 * marketDataService.js
 * ─────────────────────────────────────────────────────────────────
 * Service layer for OHLCV market data.
 *
 * Architecture:
 *  • All data access goes through this service — never fetch directly in components.
 *  • Mock mode:  returns generated sample data.
 *  • API mode:   calls the FastAPI backend via Nginx reverse proxy.
 *               The function signatures & return shapes stay the same,
 *               so the rest of the app needs zero changes.
 *
 * OHLCV candle shape expected by lightweight-charts:
 *  { time: number (unix seconds), open, high, low, close, volume }
 * ─────────────────────────────────────────────────────────────────
 */

// ─── Config ──────────────────────────────────────────────────────
// Toggle to 'mock' for local development without backend.
const DATA_SOURCE = process.env.REACT_APP_DATA_SOURCE || "api"; // 'mock' | 'api'

// Base URL of your backend REST/WebSocket endpoint.
// Defaults to '/api' for Nginx reverse-proxy setup.
// For local dev without Docker: REACT_APP_API_BASE_URL=http://localhost:8080/api
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || "/api";

// ─── Timeframe helpers ────────────────────────────────────────────
export const TIMEFRAMES = {
  "1s": { label: "1s", seconds: 1 },
  "1m": { label: "1m", seconds: 60 },
  "5m": { label: "5m", seconds: 300 },
  "15m": { label: "15m", seconds: 900 },
  "1h": { label: "1H", seconds: 3600 },
  "4h": { label: "4H", seconds: 14400 },
  "1d": { label: "1D", seconds: 86400 },
  "1w": { label: "1W", seconds: 604800 },
};

// ─── WebSocket URL helper ─────────────────────────────────────────
function getWsBaseUrl() {
  if (API_BASE_URL.startsWith("http")) {
    return API_BASE_URL.replace(/^http/, "ws");
  }
  // Relative path — construct from current page origin
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}${API_BASE_URL}`;
}

// ─── Mock data generator ──────────────────────────────────────────
/**
 * Generates a realistic OHLCV series using a random-walk model.
 * Replace this entire block — not the function signature — when switching to API.
 */
function generateMockCandles(symbol, timeframeKey, count = 200) {
  const tf = TIMEFRAMES[timeframeKey] || TIMEFRAMES["1h"];
  const now = Math.floor(Date.now() / 1000);
  const startTime = now - tf.seconds * count;

  // Seed prices per symbol so they feel realistic
  const seedPrices = {
    BTCUSDT: 64000,
    ETHUSDT: 3400,
    BNBUSDT: 580,
    SOLUSDT: 165,
    XRPUSDT: 2.35,
    DOGEUSDT: 0.158,
    ADAUSDT: 0.72,
    AVAXUSDT: 35.2,
    DOTUSDT: 7.5,
    LINKUSDT: 18.5,
    MATICUSDT: 0.72,
    LTCUSDT: 95,
    default: 100,
  };

  let price = seedPrices[symbol] || seedPrices.default;
  const volatility = price * 0.008; // 0.8% per candle std dev
  const candles = [];

  for (let i = 0; i < count; i++) {
    const time = startTime + i * tf.seconds;
    const open = price;
    const change = (Math.random() - 0.49) * volatility * 2;
    const close = Math.max(open + change, 1);
    const wick = Math.random() * volatility;
    const high = Math.max(open, close) + wick;
    const low = Math.max(Math.min(open, close) - wick, 1);
    const volume = Math.round(price * (0.5 + Math.random()) * 10);

    candles.push({
      time,
      open: +open.toFixed(2),
      high: +high.toFixed(2),
      low: +low.toFixed(2),
      close: +close.toFixed(2),
      volume,
    });
    price = close;
  }

  return candles;
}

// ─── Public API ───────────────────────────────────────────────────

/**
 * Fetch historical OHLCV candles.
 *
 * @param {string} symbol      e.g. 'BTCUSDT'
 * @param {string} timeframe   key of TIMEFRAMES, e.g. '1h'
 * @param {number} [limit=200] number of candles
 * @param {number} [endTime]   optional end timestamp in seconds (exclusive). When provided, fetches candles before this time.
 * @returns {Promise<Array>}   array of { time, open, high, low, close, volume }
 */
export async function fetchCandles(symbol, timeframe = "1h", limit = 200, endTime = null) {
  if (DATA_SOURCE === "api") {
    let url = `${API_BASE_URL}/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(timeframe)}&limit=${limit}`;
    if (endTime) {
      // Convert seconds to milliseconds for backend API
      url += `&endTime=${endTime * 1000}`;
    }
    const res = await fetch(url, { headers: { "Content-Type": "application/json" } });
    if (!res.ok) throw new Error(`API error ${res.status}`);
    const raw = await res.json();
    // Adapt this mapping to match your backend response shape:
    return raw.map((k) => ({
      time: Math.floor(k.openTime / 1000),
      open: parseFloat(k.open),
      high: parseFloat(k.high),
      low: parseFloat(k.low),
      close: parseFloat(k.close),
      volume: parseFloat(k.volume),
    }));
  }

  // Mock fallback (remove once API is ready)
  return new Promise((resolve) => {
    setTimeout(
      () => resolve(generateMockCandles(symbol, timeframe, limit)),
      300,
    );
  });
}

/**
 * Subscribe to real-time candle updates via WebSocket.
 *
 * @param {string}   symbol
 * @param {string}   timeframe
 * @param {Function} onCandle   called with latest { time, open, high, low, close, volume }
 * @returns {Function}          unsubscribe function — call it on cleanup
 */
export function subscribeCandle(symbol, timeframe, onCandle) {
  if (DATA_SOURCE === "api") {
    const wsUrl = `${getWsBaseUrl()}/stream?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(timeframe)}`;
    const ws = new WebSocket(wsUrl);
    ws.onmessage = (e) => {
      const k = JSON.parse(e.data);
      onCandle({
        time: Math.floor(k.openTime / 1000),
        open: parseFloat(k.open),
        high: parseFloat(k.high),
        low: parseFloat(k.low),
        close: parseFloat(k.close),
        volume: parseFloat(k.volume),
      });
    };
    ws.onerror = (err) => console.error("[WS error]", err);
    return () => ws.close();
  }

  // Mock: simulate a live tick every 2 seconds
  let lastCandle = null;
  const interval = setInterval(() => {
    const mockSeries = generateMockCandles(symbol, timeframe, 2);
    const latest = mockSeries[mockSeries.length - 1];
    if (!lastCandle || latest.time >= lastCandle.time) {
      lastCandle = latest;
      onCandle(latest);
    }
  }, 2000);

  return () => clearInterval(interval);
}

/**
 * Fetch available trading symbols from your backend.
 * Currently returns a static list — replace with an API call when ready.
 */
export async function fetchSymbols() {
  if (DATA_SOURCE === "api") {
    const res = await fetch(`${API_BASE_URL}/symbols`);
    if (!res.ok) throw new Error(`API error ${res.status}`);
    return res.json();
  }

  return [
    { symbol: "BTCUSDT", name: "Bitcoin / USDT", type: "crypto" },
    { symbol: "ETHUSDT", name: "Ethereum / USDT", type: "crypto" },
    { symbol: "BNBUSDT", name: "BNB / USDT", type: "crypto" },
    { symbol: "SOLUSDT", name: "Solana / USDT", type: "crypto" },
    { symbol: "XRPUSDT", name: "XRP / USDT", type: "crypto" },
    { symbol: "DOGEUSDT", name: "Dogecoin / USDT", type: "crypto" },
    { symbol: "ADAUSDT", name: "Cardano / USDT", type: "crypto" },
    { symbol: "AVAXUSDT", name: "Avalanche / USDT", type: "crypto" },
    { symbol: "DOTUSDT", name: "Polkadot / USDT", type: "crypto" },
    { symbol: "LINKUSDT", name: "Chainlink / USDT", type: "crypto" },
    { symbol: "MATICUSDT", name: "Polygon / USDT", type: "crypto" },
    { symbol: "LTCUSDT", name: "Litecoin / USDT", type: "crypto" },
  ];
}

// ─── Historical Candles (Iceberg via Trino) ───────────────────────

/**
 * Fetch historical hourly candles from Iceberg cold storage.
 *
 * @param {string} symbol      e.g. 'BTCUSDT'
 * @param {number} startMs     range start in epoch milliseconds
 * @param {number} endMs       range end in epoch milliseconds
 * @param {number} [limit=500] max candles to return
 * @returns {Promise<Array>}   array of { time, open, high, low, close, volume }
 */
export async function fetchHistoricalCandles(
  symbol,
  startMs,
  endMs,
  limit = 500,
) {
  if (DATA_SOURCE === "api") {
    const params = new URLSearchParams({
      symbol,
      startTime: String(startMs),
      endTime: String(endMs),
      limit: String(limit),
    });
    const res = await fetch(`${API_BASE_URL}/klines/historical?${params}`);
    if (!res.ok) throw new Error(`API error ${res.status}`);
    const raw = await res.json();
    return raw.map((k) => ({
      time: Math.floor(k.openTime / 1000),
      open: parseFloat(k.open),
      high: parseFloat(k.high),
      low: parseFloat(k.low),
      close: parseFloat(k.close),
      volume: parseFloat(k.volume),
    }));
  }

  // Mock: generate hourly candles for the date range
  const hourMs = 3600 * 1000;
  const count = Math.min(Math.floor((endMs - startMs) / hourMs), limit);
  return new Promise((resolve) => {
    setTimeout(
      () => resolve(generateMockCandles(symbol, "1h", Math.max(count, 10))),
      300,
    );
  });
}

// ─── Order Book ───────────────────────────────────────────────────

function generateMockOrderBook(basePrice, depth = 20) {
  const asks = [];
  const bids = [];
  let seed = Math.floor(basePrice * 100);
  function rand() {
    seed = (seed * 9301 + 49297) % 233280;
    return seed / 233280;
  }
  for (let i = 0; i < depth; i++) {
    const askP = +(
      basePrice *
      (1 + (i + 1) * 0.0005 + rand() * 0.0003)
    ).toFixed(2);
    const bidP = +(
      basePrice *
      (1 - (i + 1) * 0.0005 - rand() * 0.0003)
    ).toFixed(2);
    asks.push([askP, +(rand() * 5 + 0.1).toFixed(4)]);
    bids.push([bidP, +(rand() * 5 + 0.1).toFixed(4)]);
  }
  asks.sort((a, b) => a[0] - b[0]);
  bids.sort((a, b) => b[0] - a[0]);
  return {
    bids,
    asks,
    spread: +(asks[0][0] - bids[0][0]).toFixed(2),
    best_bid: bids[0][0],
    best_ask: asks[0][0],
  };
}

/**
 * Fetch order book for a symbol.
 * Backend returns { bids: [[price, qty], ...], asks: [[price, qty], ...], spread, best_bid, best_ask }
 */
export async function fetchOrderBook(symbol) {
  if (DATA_SOURCE === "api") {
    const res = await fetch(
      `${API_BASE_URL}/orderbook/${encodeURIComponent(symbol)}`,
    );
    if (!res.ok) throw new Error(`API error ${res.status}`);
    return res.json();
  }
  return generateMockOrderBook(symbol === "BTCUSDT" ? 64000 : 100);
}

// ─── Recent Trades ────────────────────────────────────────────────

function generateMockTrades(basePrice, count = 50) {
  const trades = [];
  let seed = Math.floor(basePrice * 37);
  function rand() {
    seed = (seed * 9301 + 49297) % 233280;
    return seed / 233280;
  }
  const now = Math.floor(Date.now() / 1000);
  let price = basePrice;
  for (let i = 0; i < count; i++) {
    const side = rand() > 0.5 ? "buy" : "sell";
    price = Math.max(price + (rand() - 0.5) * basePrice * 0.002, 1);
    trades.push({
      time: (now - (count - i) * (Math.floor(rand() * 30) + 5)) * 1000,
      price: +price.toFixed(2),
      volume: +(rand() * 3 + 0.001).toFixed(4),
      side,
    });
  }
  return trades.reverse();
}

/**
 * Fetch recent trades / price ticks.
 * Backend returns [{ time (ms), price, volume, side }]
 */
export async function fetchTrades(symbol, limit = 50) {
  if (DATA_SOURCE === "api") {
    const res = await fetch(
      `${API_BASE_URL}/trades/${encodeURIComponent(symbol)}?limit=${limit}`,
    );
    if (!res.ok) throw new Error(`API error ${res.status}`);
    return res.json();
  }
  return generateMockTrades(symbol === "BTCUSDT" ? 64000 : 100, limit);
}

// ─── Tickers ──────────────────────────────────────────────────────

/**
 * Fetch a single live ticker by symbol.
 * Returns { symbol, price, bid, ask, volume, event_time }
 */
export async function fetchTicker(symbol) {
  if (DATA_SOURCE === "api") {
    const res = await fetch(
      `${API_BASE_URL}/ticker/${encodeURIComponent(symbol)}`,
    );
    if (!res.ok) throw new Error(`API error ${res.status}`);
    return res.json();
  }
  return { symbol, price: 0 };
}

/**
 * Fetch all live tickers.
 * Backend returns [{ symbol, price, bid, ask, volume, event_time }]
 */
export async function fetchTickers() {
  if (DATA_SOURCE === "api") {
    const res = await fetch(`${API_BASE_URL}/ticker`);
    if (!res.ok) throw new Error(`API error ${res.status}`);
    return res.json();
  }
  // Mock fallback — return static prices
  return [
    { symbol: "BTCUSDT", price: 64444, change24h: 0.33 },
    { symbol: "ETHUSDT", price: 3400, change24h: -0.53 },
    { symbol: "BNBUSDT", price: 580, change24h: -0.27 },
    { symbol: "SOLUSDT", price: 165, change24h: 0.54 },
    { symbol: "XRPUSDT", price: 2.35, change24h: -0.16 },
    { symbol: "DOGEUSDT", price: 0.158, change24h: -0.6 },
    { symbol: "ADAUSDT", price: 0.72, change24h: -0.83 },
    { symbol: "AVAXUSDT", price: 35.2, change24h: 0.33 },
  ];
}
