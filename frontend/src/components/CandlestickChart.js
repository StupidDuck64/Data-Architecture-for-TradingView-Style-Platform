import React, { useEffect, useRef, useState, useCallback } from "react";
import {
  createChart,
  CrosshairMode,
  LineStyle,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
} from "lightweight-charts";
import {
  Settings,
  ChevronDown,
  ChevronUp,
  Download,
  BarChart3,
  BookOpen,
  ArrowLeftRight,
  Eye,
} from "lucide-react";
import { useI18n } from "../i18n";
import { fetchCandles } from "../services/marketDataService";
import MarketSelector from "./MarketSelector";
import OverviewChart from "./OverviewChart";
import OrderBook from "./OrderBook";
import RecentTrades from "./RecentTrades";

const THEME = {
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

// ─── Math helpers ─────────────────────────────────────────────────
const SYMBOLS = [
  "BTCUSDT",
  "ETHUSDT",
  "BNBUSDT",
  "SOLUSDT",
  "XRPUSDT",
  "DOGEUSDT",
  "ADAUSDT",
  "AVAXUSDT",
  "DOTUSDT",
  "LINKUSDT",
  "MATICUSDT",
  "LTCUSDT",
];
const TIMEFRAMES = ["1s", "1m", "5m", "15m", "1H", "4H", "1D", "1W"];
const CHART_TABS = ["candlestick", "overview", "orderBook", "recentTrades"];
const TAB_ICONS = {
  candlestick: BarChart3,
  overview: Eye,
  orderBook: BookOpen,
  recentTrades: ArrowLeftRight,
};

function calcSMA(candles, period) {
  const out = [];
  for (let i = period - 1; i < candles.length; i++) {
    const avg =
      candles.slice(i - period + 1, i + 1).reduce((s, c) => s + c.close, 0) /
      period;
    out.push({ time: candles[i].time, value: +avg.toFixed(4) });
  }
  return out;
}

function calcEMA(candles, period) {
  const k = 2 / (period + 1);
  const out = [];
  let ema = candles.slice(0, period).reduce((s, c) => s + c.close, 0) / period;
  out.push({ time: candles[period - 1].time, value: +ema.toFixed(4) });
  for (let i = period; i < candles.length; i++) {
    ema = candles[i].close * k + ema * (1 - k);
    out.push({ time: candles[i].time, value: +ema.toFixed(4) });
  }
  return out;
}

function calcRSI(candles, period) {
  const out = [];
  if (candles.length < period + 1) return out;
  let gains = 0,
    losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = candles[i].close - candles[i - 1].close;
    if (diff > 0) gains += diff;
    else losses -= diff;
  }
  let avgGain = gains / period,
    avgLoss = losses / period;
  const rsi = (v) => (avgLoss === 0 ? 100 : 100 - 100 / (1 + v));
  out.push({
    time: candles[period].time,
    value: +rsi(avgGain / avgLoss).toFixed(2),
  });
  for (let i = period + 1; i < candles.length; i++) {
    const diff = candles[i].close - candles[i - 1].close;
    const g = diff > 0 ? diff : 0;
    const l = diff < 0 ? -diff : 0;
    avgGain = (avgGain * (period - 1) + g) / period;
    avgLoss = (avgLoss * (period - 1) + l) / period;
    out.push({
      time: candles[i].time,
      value: +rsi(avgGain / (avgLoss || 1e-10)).toFixed(2),
    });
  }
  return out;
}

function calcMFI(candles, period) {
  const out = [];
  const typicals = candles.map((c) => ({
    time: c.time,
    tp: (c.high + c.low + c.close) / 3,
    vol: c.volume,
  }));
  for (let i = period; i < typicals.length; i++) {
    let posFlow = 0,
      negFlow = 0;
    for (let j = i - period + 1; j <= i; j++) {
      const mf = typicals[j].tp * typicals[j].vol;
      if (typicals[j].tp >= typicals[j - 1].tp) posFlow += mf;
      else negFlow += mf;
    }
    const ratio = negFlow === 0 ? 100 : 100 - 100 / (1 + posFlow / negFlow);
    out.push({ time: typicals[i].time, value: +ratio.toFixed(2) });
  }
  return out;
}

// ─── Indicator settings defaults ─────────────────────────────────
const DEFAULT_INDICATOR_SETTINGS = {
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

// ─── Indicator Settings Panel ─────────────────────────────────────
const IndicatorPanel = ({ indSettings, onChange }) => {
  const { t } = useI18n();
  const [expanded, setExpanded] = useState(null);

  const indicators = [
    { key: "volume", label: "Volume" },
    { key: "sma20", label: "SMA 20" },
    { key: "sma50", label: "SMA 50" },
    { key: "ema", label: "EMA" },
    { key: "rsi", label: "RSI" },
    { key: "mfi", label: "MFI" },
  ];

  const set = (key, field, value) => {
    onChange({
      ...indSettings,
      [key]: { ...indSettings[key], [field]: value },
    });
  };

  const toggleVisible = (key) => set(key, "visible", !indSettings[key].visible);

  return (
    <div className="mt-1 w-56 bg-gray-800 border border-gray-700 rounded-lg shadow-2xl overflow-hidden">
      <div className="px-3 py-2 bg-gray-750 border-b border-gray-700 text-xs font-semibold text-gray-300 flex items-center gap-1">
        <Settings size={11} /> {t("technicalIndicators")}
      </div>
      {indicators.map(({ key, label }) => {
        const cfg = indSettings[key] || {};
        const isOpen = expanded === key;
        return (
          <div key={key} className="border-b border-gray-700 last:border-0">
            <div
              className="flex items-center justify-between px-3 py-1.5 hover:bg-gray-700 cursor-pointer"
              onClick={() => setExpanded(isOpen ? null : key)}
            >
              <div className="flex items-center gap-2">
                {/* Color dot */}
                {cfg.color && (
                  <span
                    className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                    style={{ backgroundColor: cfg.color }}
                  />
                )}
                <span className="text-xs text-gray-300">{label}</span>
              </div>
              <div className="flex items-center gap-1.5">
                {/* Toggle visible */}
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    toggleVisible(key);
                  }}
                  className={`w-8 h-4 rounded-full transition-colors ${cfg.visible ? "bg-blue-600" : "bg-gray-600"}`}
                >
                  <span
                    className={`block w-3 h-3 rounded-full bg-white shadow mx-0.5 transition-transform ${cfg.visible ? "translate-x-4" : "translate-x-0"}`}
                  />
                </button>
                {isOpen ? (
                  <ChevronUp size={12} className="text-gray-400" />
                ) : (
                  <ChevronDown size={12} className="text-gray-400" />
                )}
              </div>
            </div>
            {/* Settings rows */}
            {isOpen && (
              <div className="px-3 pb-2 space-y-1.5 bg-gray-900">
                {cfg.period !== undefined && (
                  <div className="flex items-center justify-between gap-2 mt-1.5">
                    <span className="text-xs text-gray-400">{t("period")}</span>
                    <input
                      type="number"
                      min="1"
                      max="500"
                      value={cfg.period}
                      onChange={(e) =>
                        set(
                          key,
                          "period",
                          parseInt(e.target.value) || cfg.period,
                        )
                      }
                      className="w-16 bg-gray-700 text-white text-xs rounded px-2 py-0.5 border border-gray-600 focus:outline-none"
                    />
                  </div>
                )}
                {cfg.color !== undefined && (
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-xs text-gray-400">{t("color")}</span>
                    <input
                      type="color"
                      value={cfg.color}
                      onChange={(e) => set(key, "color", e.target.value)}
                      className="w-8 h-5 rounded cursor-pointer border-0 bg-transparent"
                    />
                  </div>
                )}
                {cfg.lineWidth !== undefined && (
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-xs text-gray-400">
                      {t("thickness")}
                    </span>
                    <input
                      type="range"
                      min="0.5"
                      max="4"
                      step="0.5"
                      value={cfg.lineWidth}
                      onChange={(e) =>
                        set(key, "lineWidth", parseFloat(e.target.value))
                      }
                      className="w-20 accent-blue-500"
                    />
                    <span className="text-xs text-gray-300 w-4">
                      {cfg.lineWidth}
                    </span>
                  </div>
                )}
                {cfg.overbought !== undefined && (
                  <>
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-xs text-gray-400">
                        {t("overbought")}
                      </span>
                      <input
                        type="number"
                        min="50"
                        max="100"
                        value={cfg.overbought}
                        onChange={(e) =>
                          set(key, "overbought", parseInt(e.target.value))
                        }
                        className="w-16 bg-gray-700 text-white text-xs rounded px-2 py-0.5 border border-gray-600 focus:outline-none"
                      />
                    </div>
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-xs text-gray-400">
                        {t("oversold")}
                      </span>
                      <input
                        type="number"
                        min="0"
                        max="50"
                        value={cfg.oversold}
                        onChange={(e) =>
                          set(key, "oversold", parseInt(e.target.value))
                        }
                        className="w-16 bg-gray-700 text-white text-xs rounded px-2 py-0.5 border border-gray-600 focus:outline-none"
                      />
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};

// ─── Sub-chart pane (RSI / MFI) ───────────────────────────────────
const OscillatorPane = ({ data, settings, label }) => {
  const ref = useRef(null);
  const chartRef = useRef(null);

  useEffect(() => {
    if (!ref.current || !data || data.length === 0) return;
    const chart = createChart(ref.current, {
      layout: {
        background: { color: "#141620" },
        textColor: THEME.textColor,
        fontSize: 10,
      },
      grid: {
        vertLines: { color: THEME.gridColor },
        horzLines: { color: THEME.gridColor },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: {
        borderColor: THEME.borderColor,
        scaleMargins: { top: 0.1, bottom: 0.1 },
      },
      timeScale: {
        borderColor: THEME.borderColor,
        timeVisible: true,
        secondsVisible: false,
      },
      handleScroll: false,
      handleScale: false,
    });
    const series = chart.addSeries(LineSeries, {
      color: settings.color,
      lineWidth: settings.lineWidth || 1.5,
      priceLineVisible: false,
      lastValueVisible: true,
      crosshairMarkerVisible: true,
    });
    series.setData(data);

    // Overbought / Oversold bands
    if (settings.overbought != null) {
      series.createPriceLine({
        price: settings.overbought,
        color: "#ef444460",
        lineStyle: LineStyle.Dashed,
        lineWidth: 1,
        title: "OB",
        axisLabelVisible: true,
      });
      series.createPriceLine({
        price: settings.oversold,
        color: "#22c55e60",
        lineStyle: LineStyle.Dashed,
        lineWidth: 1,
        title: "OS",
        axisLabelVisible: true,
      });
    }

    chart.timeScale().fitContent();
    chartRef.current = chart;
    const ro = new ResizeObserver(() => {
      if (ref.current)
        chart.resize(ref.current.clientWidth, ref.current.clientHeight);
    });
    ro.observe(ref.current);
    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Update data
  useEffect(() => {
    if (!chartRef.current || !data || data.length === 0) return;
    // Re-create series on settings change is handled by unmount/remount via key
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, settings]);

  return (
    <div className="relative border-t border-gray-700" style={{ height: 100 }}>
      <span className="absolute top-1 left-2 text-xs text-gray-400 z-10 pointer-events-none">
        {label}
      </span>
      <div ref={ref} className="w-full h-full" />
    </div>
  );
};

const OHLCVBar = ({ data }) => {
  if (!data) return null;
  const isUp = data.close >= data.open;
  const clr = isUp ? THEME.upColor : THEME.downColor;
  const f = (v) =>
    v != null
      ? v.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })
      : "-";
  return (
    <div className="flex items-center gap-4 text-xs font-mono select-none flex-wrap">
      {data.timeLabel && (
        <span className="text-gray-400">{data.timeLabel}</span>
      )}
      <span>
        O <span style={{ color: clr }}>{f(data.open)}</span>
      </span>
      <span>
        H <span style={{ color: clr }}>{f(data.high)}</span>
      </span>
      <span>
        L <span style={{ color: clr }}>{f(data.low)}</span>
      </span>
      <span>
        C <span style={{ color: clr }}>{f(data.close)}</span>
      </span>
      {data.volume != null && (
        <span>
          V{" "}
          <span className="text-gray-400">{data.volume.toLocaleString()}</span>
        </span>
      )}
    </div>
  );
};

const CandlestickChart = ({
  defaultSymbol = "BTCUSDT",
  symbol: symbolProp,
  children,
  starredSymbols = [],
  onToggleStar,
  onSymbolChange,
}) => {
  const { t } = useI18n();
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const candleRef = useRef(null);
  const volumeRef = useRef(null);
  const sma20Ref = useRef(null);
  const sma50Ref = useRef(null);
  const emaRef = useRef(null);

  const [symbol, setSymbol] = useState(symbolProp || defaultSymbol);
  const [timeframe, setTimeframe] = useState("1m");
  const [tooltip, setTooltip] = useState(null);
  const [showIndPanel, setShowIndPanel] = useState(false);
  const [indSettings, setIndSettings] = useState(() =>
    JSON.parse(JSON.stringify(DEFAULT_INDICATOR_SETTINGS)),
  );
  const [activeTab, setActiveTab] = useState("candlestick");
  const [candles, setCandles] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  const handleSymbolChange = useCallback(
    (s) => {
      setSymbol(s);
      if (onSymbolChange) onSymbolChange(s);
    },
    [onSymbolChange],
  );

  // Sync symbol from external prop
  useEffect(() => {
    if (symbolProp && symbolProp !== symbol) setSymbol(symbolProp);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbolProp]);

  // Init chart once
  useEffect(() => {
    if (!containerRef.current) return;
    const chart = createChart(containerRef.current, {
      layout: {
        background: { color: THEME.background },
        textColor: THEME.textColor,
        fontFamily: "'Inter','Segoe UI',sans-serif",
        fontSize: 12,
      },
      grid: {
        vertLines: { color: THEME.gridColor, style: LineStyle.Solid },
        horzLines: { color: THEME.gridColor, style: LineStyle.Solid },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: THEME.crosshair, labelBackgroundColor: "#374151" },
        horzLine: { color: THEME.crosshair, labelBackgroundColor: "#374151" },
      },
      rightPriceScale: {
        borderColor: THEME.borderColor,
        scaleMargins: { top: 0.05, bottom: 0.25 },
      },
      timeScale: {
        borderColor: THEME.borderColor,
        timeVisible: true,
        secondsVisible: false,
        barSpacing: 8,
        minBarSpacing: 3,
      },
      handleScroll: { mouseWheel: true, pressedMouseMove: true },
      handleScale: {
        axisPressedMouseMove: true,
        mouseWheel: true,
        pinch: true,
      },
    });
    const cs = chart.addSeries(CandlestickSeries, {
      upColor: THEME.upColor,
      downColor: THEME.downColor,
      borderUpColor: THEME.upColor,
      borderDownColor: THEME.downColor,
      wickUpColor: THEME.upColor,
      wickDownColor: THEME.downColor,
    });
    const vs = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    });
    chart
      .priceScale("volume")
      .applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    const s20 = chart.addSeries(LineSeries, {
      color: THEME.sma20,
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: true,
      crosshairMarkerVisible: false,
    });
    const s50 = chart.addSeries(LineSeries, {
      color: THEME.sma50,
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: true,
      crosshairMarkerVisible: false,
    });
    const ema = chart.addSeries(LineSeries, {
      color: THEME.ema,
      lineWidth: 1.5,
      priceLineVisible: false,
      lastValueVisible: true,
      crosshairMarkerVisible: false,
      visible: false,
    });
    chartRef.current = chart;
    candleRef.current = cs;
    volumeRef.current = vs;
    sma20Ref.current = s20;
    sma50Ref.current = s50;
    emaRef.current = ema;
    chart.subscribeCrosshairMove((param) => {
      if (!param.time || (param.point && param.point.x < 0)) {
        setTooltip(null);
        return;
      }
      const c = param.seriesData.get(cs);
      const v = param.seriesData.get(vs);
      if (c) {
        const d = new Date(param.time * 1000);
        const lbl = d.toLocaleString("en-US", {
          month: "short",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
        });
        setTooltip({ ...c, volume: v ? v.value : null, timeLabel: lbl });
      }
    });
    const ro = new ResizeObserver(() => {
      if (containerRef.current)
        chart.resize(
          containerRef.current.clientWidth,
          containerRef.current.clientHeight,
        );
    });
    ro.observe(containerRef.current);
    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Load data when symbol or timeframe changes
  useEffect(() => {
    if (!candleRef.current) return;
    setIsLoading(true);
    fetchCandles(symbol, timeframe.toLowerCase(), 120)
      .then((data) => {
        if (!candleRef.current) return;
        setCandles(data);
        candleRef.current.setData(data);
        const vs = volumeRef.current;
        if (vs)
          vs.setData(
            data.map((c) => ({
              time: c.time,
              value: c.volume,
              color: c.close >= c.open ? THEME.volumeUp : THEME.volumeDown,
            })),
          );
        if (sma20Ref.current)
          sma20Ref.current.setData(calcSMA(data, indSettings.sma20.period));
        if (sma50Ref.current)
          sma50Ref.current.setData(calcSMA(data, indSettings.sma50.period));
        if (emaRef.current)
          emaRef.current.setData(calcEMA(data, indSettings.ema.period));
        if (chartRef.current) chartRef.current.timeScale().fitContent();
        setTooltip({ ...data[data.length - 1], timeLabel: "" });
        setIsLoading(false);
      })
      .catch(() => setIsLoading(false));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol, timeframe]);

  // Re-layout chart when candlestick tab becomes visible again
  useEffect(() => {
    if (
      activeTab === "candlestick" &&
      chartRef.current &&
      containerRef.current
    ) {
      const { width, height } = containerRef.current.getBoundingClientRect();
      if (width > 0 && height > 0) {
        chartRef.current.resize(width, height);
        chartRef.current.timeScale().fitContent();
      }
    }
  }, [activeTab]);

  // Apply indicator settings (visibility, color, period, lineWidth)
  useEffect(() => {
    if (candles.length === 0) return;
    const cfg20 = indSettings.sma20;
    const cfg50 = indSettings.sma50;
    const cfgE = indSettings.ema;
    const cfgV = indSettings.volume;
    if (sma20Ref.current) {
      sma20Ref.current.applyOptions({
        visible: cfg20.visible,
        color: cfg20.color,
        lineWidth: cfg20.lineWidth,
      });
      sma20Ref.current.setData(calcSMA(candles, cfg20.period));
    }
    if (sma50Ref.current) {
      sma50Ref.current.applyOptions({
        visible: cfg50.visible,
        color: cfg50.color,
        lineWidth: cfg50.lineWidth,
      });
      sma50Ref.current.setData(calcSMA(candles, cfg50.period));
    }
    if (emaRef.current) {
      emaRef.current.applyOptions({
        visible: cfgE.visible,
        color: cfgE.color,
        lineWidth: cfgE.lineWidth,
      });
      emaRef.current.setData(calcEMA(candles, cfgE.period));
    }
    if (volumeRef.current)
      volumeRef.current.applyOptions({ visible: cfgV.visible });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [indSettings, candles]);

  const lastCandle = candles[candles.length - 1];
  const firstCandle = candles[0];
  const priceDiff =
    lastCandle && firstCandle ? lastCandle.close - firstCandle.open : 0;
  const pricePct = firstCandle
    ? ((priceDiff / firstCandle.open) * 100).toFixed(2)
    : null;
  const isUp = priceDiff >= 0;

  // RSI / MFI computed data
  const rsiData = indSettings.rsi.visible
    ? calcRSI(candles, indSettings.rsi.period)
    : null;
  const mfiData = indSettings.mfi.visible
    ? calcMFI(candles, indSettings.mfi.period)
    : null;

  const handleExportChart = useCallback(() => {
    if (!containerRef.current) return;
    const canvas = containerRef.current.querySelector("canvas");
    if (!canvas) return;
    const link = document.createElement("a");
    link.download = `${symbol}_${timeframe}_chart.png`;
    link.href = canvas.toDataURL("image/png");
    link.click();
  }, [symbol, timeframe]);

  const TFBtn = useCallback(
    ({ tf }) => (
      <button
        onClick={() => setTimeframe(tf)}
        className={`px-2 py-0.5 rounded text-xs font-medium transition-colors ${timeframe === tf ? "bg-blue-600 text-white" : "text-gray-400 hover:text-white hover:bg-gray-700"}`}
      >
        {tf}
      </button>
    ),
    [timeframe],
  );

  return (
    <div className="flex flex-col h-full bg-gray-900 rounded-lg overflow-hidden">
      {/* Top bar */}
      <div className="flex items-center justify-between px-3 py-2 bg-gray-800 border-b border-gray-700 flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <MarketSelector
            symbols={SYMBOLS}
            selectedSymbol={symbol}
            onSelect={handleSymbolChange}
            starredSymbols={starredSymbols}
            onToggleStar={onToggleStar || (() => {})}
          />
          {lastCandle && (
            <div className="flex items-center gap-2">
              <span
                className={`text-base font-bold font-mono ${isUp ? "text-green-400" : "text-red-400"}`}
              >
                {lastCandle.close.toLocaleString(undefined, {
                  minimumFractionDigits: 2,
                })}
              </span>
              {pricePct && (
                <span
                  className={`text-xs px-1.5 py-0.5 rounded ${isUp ? "bg-green-900 text-green-300" : "bg-red-900 text-red-300"}`}
                >
                  {isUp ? "+" : ""}
                  {pricePct}%
                </span>
              )}
            </div>
          )}
        </div>
        <div className="flex items-center gap-1">
          {TIMEFRAMES.map((tf) => (
            <TFBtn key={tf} tf={tf} />
          ))}
        </div>
        <div className="flex items-center gap-2">
          {/* Export button */}
          <button
            onClick={handleExportChart}
            className="flex items-center gap-1 px-2 py-1 rounded text-xs font-medium border border-gray-600 text-gray-400 hover:text-white hover:border-gray-400 transition-colors"
            title={t("exportAsPNG")}
          >
            <Download size={12} /> {t("exportChart")}
          </button>
          {/* Indicator panel toggle */}
          <div className="relative">
            <button
              onClick={() => setShowIndPanel((v) => !v)}
              className={`flex items-center gap-1 px-2 py-1 rounded text-xs font-medium border transition-colors
                ${showIndPanel ? "bg-blue-600 border-blue-500 text-white" : "border-gray-600 text-gray-400 hover:text-white hover:border-gray-400"}`}
            >
              <Settings size={12} /> {t("indicators")}
            </button>
            {showIndPanel && (
              <div className="absolute right-0 top-full mt-1 z-[100]">
                <IndicatorPanel
                  indSettings={indSettings}
                  onChange={setIndSettings}
                />
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Chart tabs */}
      <div className="flex items-center gap-0.5 px-3 py-1 bg-gray-800 border-b border-gray-700">
        {CHART_TABS.map((tab) => {
          const Icon = TAB_ICONS[tab];
          return (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`flex items-center gap-1 px-3 py-1 rounded text-xs font-medium transition-colors ${
                activeTab === tab
                  ? "bg-blue-600 text-white"
                  : "text-gray-400 hover:text-white hover:bg-gray-700"
              }`}
            >
              {Icon && <Icon size={12} />}
              {t(tab)}
            </button>
          );
        })}
      </div>

      {/* Tab content — candlestick chart is always mounted to preserve the
           lightweight-charts instance; visibility is toggled via CSS. */}
      <div
        style={{ display: activeTab === "candlestick" ? "contents" : "none" }}
      >
        {/* OHLCV bar */}
        <div className="px-3 py-1 bg-gray-900 border-b border-gray-800 min-h-[28px]">
          <OHLCVBar data={tooltip} />
        </div>
        {/* Chart canvas + overlay slot */}
        <div className="relative flex-1 min-h-0">
          <div ref={containerRef} className="w-full h-full" />
          {isLoading && (
            <div className="absolute inset-0 flex items-center justify-center bg-gray-900 bg-opacity-60 z-10">
              <span className="text-gray-400 text-sm animate-pulse">
                Loading…
              </span>
            </div>
          )}
          {children}
        </div>
        {/* Oscillator panes */}
        {rsiData && rsiData.length > 0 && (
          <OscillatorPane
            key={`rsi-${indSettings.rsi.period}-${indSettings.rsi.color}`}
            data={rsiData}
            settings={indSettings.rsi}
            label={`RSI(${indSettings.rsi.period})`}
          />
        )}
        {mfiData && mfiData.length > 0 && (
          <OscillatorPane
            key={`mfi-${indSettings.mfi.period}-${indSettings.mfi.color}`}
            data={mfiData}
            settings={indSettings.mfi}
            label={`MFI(${indSettings.mfi.period})`}
          />
        )}
      </div>

      {activeTab === "overview" && (
        <div className="flex-1 min-h-0">
          <OverviewChart symbol={symbol} candles={candles} />
        </div>
      )}

      {activeTab === "orderBook" && (
        <div className="flex-1 min-h-0">
          <OrderBook
            symbol={symbol}
            lastPrice={lastCandle ? lastCandle.close : 100}
          />
        </div>
      )}

      {activeTab === "recentTrades" && (
        <div className="flex-1 min-h-0">
          <RecentTrades
            symbol={symbol}
            lastPrice={lastCandle ? lastCandle.close : 100}
          />
        </div>
      )}
    </div>
  );
};

export { SYMBOLS };
export default CandlestickChart;
