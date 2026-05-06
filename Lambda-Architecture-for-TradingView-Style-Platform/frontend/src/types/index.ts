// ─────────────────────────────────────────────────────────────────────────────
// Shared type definitions for the trading dashboard frontend
// ─────────────────────────────────────────────────────────────────────────────

/** OHLCV candlestick data point (time in seconds for lightweight-charts) */
export interface Candle {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

/** Raw candle from API (time may be in ms before conversion) */
export interface RawCandle {
  openTime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

/** 24h ticker snapshot */
export interface Ticker {
  symbol: string;
  price: number;
  change24h?: number;
  bid?: number;
  ask?: number;
  volume?: number;
  event_time?: number;
}

/** Single order book price level */
export interface OrderBookEntry {
  price: number;
  amount: number;
  total: number;
}

/** Full order book snapshot */
export interface OrderBookData {
  bids: OrderBookEntry[];
  asks: OrderBookEntry[];
  spread: number;
  best_bid?: number;
  best_ask?: number;
}

/** Single trade */
export interface Trade {
  time: number;
  price: number;
  volume: number;
  side: "buy" | "sell";
}

/** Symbol info from /api/symbols */
export interface SymbolInfo {
  symbol: string;
  name?: string;
  type?: string;
}

/** Symbol metadata (icon, name, category) */
export interface SymbolMeta {
  icon: string;
  category: string;
  name: string;
  logoUrl?: string;
}

/** Watchlist item for sidebar */
export interface WatchlistItem {
  symbol: string;
  price: number;
  change: number;
  color: "green" | "red" | "gray";
}

/** Historical date range selection */
export interface HistoricalRange {
  startMs: number;
  endMs: number;
}

/** System health API response */
export interface HealthData {
  status: string;
  checks?: Record<string, string>;
  latency_ms?: Record<string, number>;
  uptime_sec?: number;
  checked_at?: string;
  total_latency_ms?: number;
  api_rtt_ms?: number;
}

/** Flexible per-indicator settings (SMA, EMA, RSI, MFI, Volume, etc.) */
export interface IndicatorSettings {
  visible: boolean;
  period?: number;
  color?: string;
  lineWidth?: number;
  type?: string;
  overbought?: number;
  oversold?: number;
  upColor?: string;
  downColor?: string;
  [key: string]: unknown;
}

/** Point on the chart canvas for drawing tools */
export interface DrawingPoint {
  x: number;
  y: number;
}

/** Drawing object */
export interface Drawing {
  id: string | number;
  tool: string;
  start?: DrawingPoint;
  end?: DrawingPoint;
  points?: DrawingPoint[];
  settings?: Record<string, any>;
  text?: string;
}

/** Crosshair tooltip data */
export interface TooltipData {
  visible: boolean;
  x: number;
  y: number;
  time?: number;
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  volume?: number;
}

/** User session */
export interface UserSession {
  email: string;
  name: string;
}

/** Auth operation result */
export interface AuthResult {
  success: boolean;
  error?: string;
}

/** Supported timeframe string literals */
export type Timeframe = "1s" | "1m" | "5m" | "15m" | "1H" | "4H" | "1D" | "1W";

/** Watchlist filter mode */
export type WatchlistFilter = "all" | "starred";
