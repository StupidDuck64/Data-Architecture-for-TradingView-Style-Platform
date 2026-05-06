import React from "react";
import { useI18n } from "../i18n";
import { useSymbolMeta } from "../hooks/useSymbolMeta";
import { TrendingUp, TrendingDown } from "lucide-react";
import type { Candle } from "../types";

interface OverviewChartProps {
  symbol: string;
  candles: Candle[];
}

const OverviewChart: React.FC<OverviewChartProps> = ({ symbol, candles }) => {
  const { t } = useI18n();
  const { getMeta } = useSymbolMeta();

  if (!candles || candles.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-500 text-sm">
        {t("noData")}
      </div>
    );
  }

  const last = candles[candles.length - 1];
  const first = candles[0];
  const high24 = Math.max(...candles.map((c) => c.high));
  const low24 = Math.min(...candles.map((c) => c.low));
  const totalVol = candles.reduce((s, c) => s + c.volume, 0);
  const change = last.close - first.open;
  const changePct = ((change / first.open) * 100).toFixed(2);
  const isUp = change >= 0;
  const meta = getMeta(symbol);

  // Price position within 24h range (for range bar)
  const rangeSpan = high24 - low24 || 1;
  const positionPct = ((last.close - low24) / rangeSpan) * 100;

  // Mini sparkline
  const prices = candles.map((c) => c.close);
  const minP = Math.min(...prices);
  const maxP = Math.max(...prices);
  const pRange = maxP - minP || 1;
  const w = 200;
  const h = 40;
  const points = prices
    .map((p, i) => {
      const x = (i / (prices.length - 1)) * w;
      const y = h - ((p - minP) / pRange) * h;
      return `${x},${y}`;
    })
    .join(" ");

  const f = (v: number | null | undefined) =>
    v != null
      ? v.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })
      : "-";
  const fCompact = (v: number) => {
    if (v >= 1e9) return (v / 1e9).toFixed(2) + "B";
    if (v >= 1e6) return (v / 1e6).toFixed(2) + "M";
    if (v >= 1e3) return (v / 1e3).toFixed(1) + "K";
    return v.toFixed(2);
  };

  return (
    <div className="h-full flex flex-col p-3 overflow-y-auto">
      {/* Header: symbol + live price */}
      <div className="flex items-center gap-2 mb-2">
        {meta?.logoUrl ? (
          <img
            src={meta.logoUrl}
            alt={meta.name}
            className="w-6 h-6 rounded-full"
            onError={(e) => {
              (e.target as HTMLImageElement).style.display = "none";
            }}
          />
        ) : (
          <span className="w-6 h-6 rounded-full bg-gray-600 flex items-center justify-center text-xs text-gray-300">
            {symbol.charAt(0)}
          </span>
        )}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-sm font-bold text-white truncate">
              {symbol}
            </span>
            {meta?.name && (
              <span className="text-xs text-gray-500 truncate">
                {meta.name}
              </span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-1">
          {isUp ? (
            <TrendingUp size={14} className="text-green-400" />
          ) : (
            <TrendingDown size={14} className="text-red-400" />
          )}
          <span
            className={`text-xs font-semibold px-1.5 py-0.5 rounded ${isUp ? "bg-green-900/60 text-green-300" : "bg-red-900/60 text-red-300"}`}
          >
            {isUp ? "+" : ""}
            {changePct}%
          </span>
        </div>
      </div>

      {/* Live price */}
      <div
        className={`text-xl font-bold font-mono mb-2 ${isUp ? "text-green-400" : "text-red-400"}`}
      >
        {f(last.close)}
      </div>

      {/* Mini sparkline */}
      <div className="bg-gray-900/50 rounded p-1.5 mb-3">
        <svg
          viewBox={`0 0 ${w} ${h}`}
          className="w-full"
          style={{ height: 40 }}
        >
          <defs>
            <linearGradient
              id={`ov-grad-${symbol}`}
              x1="0"
              y1="0"
              x2="0"
              y2="1"
            >
              <stop
                offset="0%"
                stopColor={isUp ? "#26a69a" : "#ef5350"}
                stopOpacity={0.25}
              />
              <stop
                offset="100%"
                stopColor={isUp ? "#26a69a" : "#ef5350"}
                stopOpacity={0}
              />
            </linearGradient>
          </defs>
          <polygon
            points={`0,${h} ${points} ${w},${h}`}
            fill={`url(#ov-grad-${symbol})`}
          />
          <polyline
            points={points}
            fill="none"
            stroke={isUp ? "#26a69a" : "#ef5350"}
            strokeWidth="1.5"
          />
        </svg>
      </div>

      {/* 24h Range bar */}
      <div className="mb-3">
        <div className="flex justify-between text-xs text-gray-500 mb-1">
          <span>{t("low24h")}</span>
          <span>{t("high24h")}</span>
        </div>
        <div className="relative h-1.5 bg-gray-700 rounded-full">
          <div
            className={`absolute top-0 left-0 h-full rounded-full ${isUp ? "bg-green-500" : "bg-red-500"}`}
            style={{ width: `${positionPct}%` }}
          />
          <div
            className="absolute top-1/2 -translate-y-1/2 w-2.5 h-2.5 rounded-full bg-white border-2 border-gray-900"
            style={{
              left: `${positionPct}%`,
              transform: `translate(-50%, -50%)`,
            }}
          />
        </div>
        <div className="flex justify-between text-xs font-mono text-gray-400 mt-1">
          <span>{f(low24)}</span>
          <span>{f(high24)}</span>
        </div>
      </div>

      {/* OHLCV stats — compact 2-col grid */}
      <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-xs">
        {[
          { label: t("open"), value: f(first.open) },
          {
            label: t("close"),
            value: f(last.close),
            color: isUp ? "text-green-400" : "text-red-400",
          },
          { label: t("high"), value: f(last.high) },
          { label: t("low"), value: f(last.low) },
          { label: t("volume24h"), value: fCompact(totalVol) },
          {
            label: t("change24h"),
            value: `${isUp ? "+" : ""}${changePct}%`,
            color: isUp ? "text-green-400" : "text-red-400",
          },
        ].map((stat) => (
          <div
            key={stat.label}
            className="flex justify-between items-center py-1 border-b border-gray-700/50"
          >
            <span className="text-gray-500">{stat.label}</span>
            <span
              className={`font-mono font-medium ${stat.color || "text-gray-300"}`}
            >
              {stat.value}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
};

export default OverviewChart;
