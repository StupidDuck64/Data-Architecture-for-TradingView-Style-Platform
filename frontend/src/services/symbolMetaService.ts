/**
 * symbolMetaService.ts
 *
 * Fetches symbol logos and full names from CoinGecko's free API,
 * with localStorage caching (24h TTL) and a bundled fallback map.
 */

import fallbackSymbolMeta, { type SymbolMetaEntry } from "../data/fallbackSymbolMeta";

const CACHE_KEY = "symbol_meta_cache";
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
const CG_BASE = "https://api.coingecko.com/api/v3";

interface CoinGeckoMarketItem {
  id: string;
  symbol: string;
  name: string;
  image: string;
}

interface CachedMeta {
  timestamp: number;
  data: Record<string, SymbolMetaEntry>;
}

/**
 * Extract the base coin symbol from a Binance trading pair.
 * e.g. "BTCUSDT" → "BTC", "1INCHUSDT" → "1INCH"
 */
function extractBaseSymbol(pair: string): string {
  const suffixes = ["USDT", "BUSD", "USDC", "BTC", "ETH", "BNB"];
  for (const suffix of suffixes) {
    if (pair.endsWith(suffix) && pair.length > suffix.length) {
      return pair.slice(0, -suffix.length);
    }
  }
  return pair;
}

/**
 * Fetch top coins from CoinGecko /coins/markets endpoint.
 * Two pages of 250 = top 500 coins by market cap.
 */
async function fetchFromCoinGecko(): Promise<Record<string, SymbolMetaEntry>> {
  const result: Record<string, SymbolMetaEntry> = {};

  for (const page of [1, 2]) {
    const url = `${CG_BASE}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=${page}&sparkline=false`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`CoinGecko API error ${res.status}`);

    const items: CoinGeckoMarketItem[] = await res.json();
    for (const item of items) {
      const key = item.symbol.toUpperCase();
      result[key] = {
        id: item.id,
        name: item.name,
        symbol: key,
        logoUrl: item.image,
        category: "crypto",
      };
    }
  }

  return result;
}

/**
 * Read cached metadata from localStorage.
 */
function readCache(): Record<string, SymbolMetaEntry> | null {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const cached: CachedMeta = JSON.parse(raw);
    if (Date.now() - cached.timestamp > CACHE_TTL_MS) return null;
    return cached.data;
  } catch {
    return null;
  }
}

/**
 * Write metadata to localStorage cache.
 */
function writeCache(data: Record<string, SymbolMetaEntry>): void {
  try {
    const cached: CachedMeta = { timestamp: Date.now(), data };
    localStorage.setItem(CACHE_KEY, JSON.stringify(cached));
  } catch {
    // Storage full or unavailable — silently ignore
  }
}

/**
 * Get symbol metadata, using cache → CoinGecko API → fallback.
 */
export async function getSymbolMetadata(): Promise<Record<string, SymbolMetaEntry>> {
  // 1. Check cache
  const cached = readCache();
  if (cached && Object.keys(cached).length > 50) return cached;

  // 2. Try CoinGecko API
  try {
    const fresh = await fetchFromCoinGecko();
    // Merge with fallback to ensure we have everything
    const merged = { ...fallbackSymbolMeta, ...fresh };
    writeCache(merged);
    return merged;
  } catch {
    // 3. Fall back to bundled static data
    return { ...fallbackSymbolMeta };
  }
}

/**
 * Look up metadata for a specific Binance trading pair.
 */
export function lookupSymbol(
  meta: Record<string, SymbolMetaEntry>,
  pair: string,
): SymbolMetaEntry | null {
  const base = extractBaseSymbol(pair);
  return meta[base] || null;
}

export type { SymbolMetaEntry };
