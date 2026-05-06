/**
 * useCandlestickData.js
 * ─────────────────────────────────────────────────────────────────
 * Custom hook that manages candlestick data lifecycle:
 *  • Initial historical fetch
 *  • Real-time streaming subscription
 *  • Loading / error states
 *  • Re-fetches automatically when symbol or timeframe changes
 * ─────────────────────────────────────────────────────────────────
 */

import { useState, useEffect, useCallback } from "react";
import { fetchCandles, subscribeCandle } from "../services/marketDataService";

/**
 * @param {string} symbol     e.g. 'BTCUSD'
 * @param {string} timeframe  e.g. '1h'
 * @param {number} limit      number of historical candles to load
 *
 * @returns {{
 *   candles:      Array,   — historical OHLCV array
 *   liveCandle:   object|null — most recent real-time update
 *   isLoading:    boolean,
 *   error:        string|null,
 *   refetch:      Function  — manually re-trigger fetch
 * }}
 */
export default function useCandlestickData(
  symbol,
  timeframe = "1h",
  limit = 1500,  // Increased from 200 to 1500 for more historical data
) {
  const [candles, setCandles] = useState([]);
  const [liveCandle, setLiveCandle] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  const load = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await fetchCandles(symbol, timeframe, limit);
      setCandles(data);
    } catch (e) {
      setError(e.message || "Failed to fetch candles");
    } finally {
      setIsLoading(false);
    }
  }, [symbol, timeframe, limit]);

  // Fetch historical data whenever symbol / timeframe changes
  useEffect(() => {
    setCandles([]);
    setLiveCandle(null);
    load();
  }, [load]);

  // Subscribe to real-time updates
  useEffect(() => {
    if (!symbol || !timeframe) return;
    const unsubscribe = subscribeCandle(symbol, timeframe, (candle) => {
      setLiveCandle(candle);
    });
    return unsubscribe;
  }, [symbol, timeframe]);

  return { candles, liveCandle, isLoading, error, refetch: load };
}
