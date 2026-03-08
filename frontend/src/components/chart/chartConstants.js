import { BarChart3, Eye, BookOpen, ArrowLeftRight } from "lucide-react";

export const THEME = {
  background: "#1a1d26",
  textColor: "#9ca3af",
  gridColor: "#2d2f3e",
  borderColor: "#374151",
  upColor: "#26a69a",
  downColor: "#ef5350",
  volumeUp: "rgba(38,166,154,0.35)",
  volumeDown: "rgba(239,83,80,0.35)",
  sma20: "#f59e0b",
  sma50: "#8b5cf6",
  ema: "#06b6d4",
  rsi: "#a78bfa",
  mfi: "#34d399",
  crosshair: "#6b7280",
};

export const TIMEFRAMES = ["1s", "1m", "5m", "15m", "1H", "4H", "1D", "1W"];

/**
 * Custom tick-mark formatter for lightweight-charts time scale.
 * Accepts raw UTC-second timestamps and formats them in the browser's
 * local timezone via Intl so the chart shows local time without having
 * to shift timestamp values in the data itself.
 *
 * tickMarkType enum (lightweight-charts 5.x):
 *   0 = Year, 1 = Month, 2 = DayOfMonth, 3 = Time, 4 = TimeWithSeconds
 */
export function localTickMarkFormatter(time, tickMarkType, locale) {
  const d = new Date(time * 1000);
  switch (tickMarkType) {
    case 0:
      return d.toLocaleDateString(locale, { year: "numeric" });
    case 1:
      return d.toLocaleDateString(locale, { month: "short", year: "numeric" });
    case 2:
      return d.toLocaleDateString(locale, { month: "short", day: "numeric" });
    case 3:
      return d.toLocaleTimeString(locale, {
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });
    case 4:
      return d.toLocaleTimeString(locale, {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      });
    default:
      return d.toLocaleDateString(locale);
  }
}

// Real-time polling interval (ms) — constant for all timeframes
export const REALTIME_POLL_MS = 2000;

export const CHART_TABS = [
  "candlestick",
  "overview",
  "orderBook",
  "recentTrades",
];

export const TAB_ICONS = {
  candlestick: BarChart3,
  overview: Eye,
  orderBook: BookOpen,
  recentTrades: ArrowLeftRight,
};

export const DEFAULT_INDICATOR_SETTINGS = {
  sma20: {
    period: 20,
    color: THEME.sma20,
    lineWidth: 1,
    visible: true,
    type: "SMA",
  },
  sma50: {
    period: 50,
    color: THEME.sma50,
    lineWidth: 1,
    visible: true,
    type: "SMA",
  },
  ema: {
    period: 20,
    color: THEME.ema,
    lineWidth: 1.5,
    visible: false,
    type: "EMA",
  },
  volume: {
    visible: true,
    upColor: THEME.volumeUp,
    downColor: THEME.volumeDown,
  },
  rsi: {
    period: 14,
    overbought: 70,
    oversold: 30,
    color: THEME.rsi,
    visible: false,
  },
  mfi: {
    period: 14,
    overbought: 80,
    oversold: 20,
    color: THEME.mfi,
    visible: false,
  },
};
