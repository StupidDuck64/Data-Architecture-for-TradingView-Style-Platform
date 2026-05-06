import type { LucideIcon } from "lucide-react";
import { BarChart3, BookOpen, ArrowLeftRight } from "lucide-react";
import type { IndicatorSettings } from "../../types";

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
} as const;

export const TIMEFRAMES = ["1s", "1m", "5m", "15m", "1H", "4H", "1D", "1W"] as const;

/**
 * Custom tick-mark formatter for lightweight-charts time scale.
 */
export function localTickMarkFormatter(
  time: number,
  tickMarkType: number,
  locale: string,
): string {
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

/**
 * Crosshair / tooltip time formatter.
 */
export function localTimeFormatter(time: number): string {
  const d = new Date(time * 1000);
  return d.toLocaleString(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

// Real-time polling interval (ms)
export const REALTIME_POLL_MS = 2000;

export const CHART_TABS = ["chart", "orderBook", "recentTrades"] as const;
export type ChartTab = (typeof CHART_TABS)[number];

export const TAB_ICONS: Record<ChartTab, LucideIcon> = {
  chart: BarChart3,
  orderBook: BookOpen,
  recentTrades: ArrowLeftRight,
};

export const DEFAULT_INDICATOR_SETTINGS: Record<string, IndicatorSettings> = {
  sma20: { period: 20, color: THEME.sma20, lineWidth: 1, visible: true, type: "SMA" },
  sma50: { period: 50, color: THEME.sma50, lineWidth: 1, visible: true, type: "SMA" },
  ema: { period: 20, color: THEME.ema, lineWidth: 1.5, visible: false, type: "EMA" },
  volume: { visible: true, upColor: THEME.volumeUp, downColor: THEME.volumeDown },
  rsi: { period: 14, overbought: 70, oversold: 30, color: THEME.rsi, visible: false },
  mfi: { period: 14, overbought: 80, oversold: 20, color: THEME.mfi, visible: false },
};
