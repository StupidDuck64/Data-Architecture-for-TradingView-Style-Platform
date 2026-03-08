import React, { useEffect, useRef, useState, useCallback } from "react";
import {
  createChart,
  CrosshairMode,
  LineStyle,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
} from "lightweight-charts";
import { Settings, Download } from "lucide-react";
import { useI18n } from "../i18n";
import {
  fetchCandles,
  fetchTicker,
  fetchHistoricalCandles,
} from "../services/marketDataService";
import MarketSelector from "./MarketSelector";
import DateRangePicker from "./DateRangePicker";
import OverviewChart from "./OverviewChart";
import OrderBook from "./OrderBook";
import RecentTrades from "./RecentTrades";
import {
  THEME,
  TIMEFRAMES,
  CHART_TABS,
  TAB_ICONS,
  DEFAULT_INDICATOR_SETTINGS,
  localTickMarkFormatter,
} from "./chart/chartConstants";
import { calcSMA, calcEMA, calcRSI, calcMFI } from "./chart/indicatorUtils";
import IndicatorPanel from "./chart/IndicatorPanel";
import OscillatorPane from "./chart/OscillatorPane";
import OHLCVBar from "./chart/OHLCVBar";

const CandlestickChart = ({
  defaultSymbol = "BTCUSDT",
  symbol: symbolProp,
  symbols = [],
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
  const candlesRef = useRef([]);

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
  const [fetchError, setFetchError] = useState(null);
  const [retryCount, setRetryCount] = useState(0);
  const [historicalRange, setHistoricalRange] = useState(null); // { startMs, endMs } or null
  const [noData, setNoData] = useState(false);

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
      localization: {
        locale: navigator.language || "en-US",
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
        tickMarkFormatter: localTickMarkFormatter,
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
        const lbl = d.toLocaleString(undefined, {
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

  // Helper: push OHLCV data into chart series + indicators
  const applyDataToChart = useCallback(
    (data) => {
      if (!candleRef.current) return;
      setCandles(data);
      candlesRef.current = data;
      setNoData(data.length === 0);
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
      if (data.length > 0)
        setTooltip({ ...data[data.length - 1], timeLabel: "" });
    },
    [indSettings],
  );

  // Load data when symbol or timeframe changes (live mode) + auto-refresh
  useEffect(() => {
    if (!candleRef.current || historicalRange) return;

    // Update secondsVisible based on timeframe
    if (chartRef.current) {
      const showSeconds = timeframe === "1s" || timeframe === "1m";
      chartRef.current
        .timeScale()
        .applyOptions({ secondsVisible: showSeconds });
    }

    // Full load — fetches 200 candles, rebuilds all series + indicators
    const loadData = () => {
      setFetchError(null);
      fetchCandles(symbol, timeframe.toLowerCase(), 200)
        .then((data) => {
          applyDataToChart(data);
          setIsLoading(false);
        })
        .catch(() => {
          setIsLoading(false);
          setFetchError("Failed to load candle data");
        });
    };

    // Lightweight tick — fetches live ticker price and patches the last
    // candle's close/high/low so it visually "agitates" in real time.
    const tickUpdate = () => {
      fetchTicker(symbol)
        .then((ticker) => {
          if (!ticker || !candleRef.current) return;
          const prev = candlesRef.current;
          if (prev.length === 0) return;
          const price = ticker.price;
          const last = { ...prev[prev.length - 1] };
          last.close = price;
          last.high = Math.max(last.high, price);
          last.low = Math.min(last.low, price);
          candleRef.current.update(last);
          if (volumeRef.current) {
            volumeRef.current.update({
              time: last.time,
              value: last.volume,
              color: price >= last.open ? THEME.volumeUp : THEME.volumeDown,
            });
          }
          const next = [...prev];
          next[next.length - 1] = last;
          candlesRef.current = next;
          setCandles(next);
          setTooltip((tip) =>
            tip ? { ...tip, ...last, timeLabel: tip.timeLabel } : null,
          );
        })
        .catch(() => {}); // silent — full refresh will recover
    };

    setIsLoading(true);
    setNoData(false);
    loadData();

    // Full sync every 30 s (recalculates indicators)
    const fullPollId = setInterval(loadData, 30000);
    // Live tick every 1 s for candle agitation
    const tickPollId = setInterval(tickUpdate, 1000);
    return () => {
      clearInterval(fullPollId);
      clearInterval(tickPollId);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol, timeframe, historicalRange, retryCount]);

  // Load historical data from Iceberg when date range is set
  useEffect(() => {
    if (!candleRef.current || !historicalRange) return;
    setIsLoading(true);
    setFetchError(null);
    fetchHistoricalCandles(
      symbol,
      historicalRange.startMs,
      historicalRange.endMs,
      2000,
    )
      .then((data) => {
        applyDataToChart(data);
        setIsLoading(false);
      })
      .catch(() => {
        setIsLoading(false);
        setFetchError("Failed to load historical data");
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol, historicalRange, retryCount]);

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

  // RSI / MFI computed data (shifted for local timezone)
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
            symbols={symbols}
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
          {historicalRange ? (
            <span className="text-xs text-amber-400 font-medium px-2">
              1H (historical)
            </span>
          ) : (
            TIMEFRAMES.map((tf) => <TFBtn key={tf} tf={tf} />)
          )}
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
          {/* Historical date-range picker */}
          <DateRangePicker
            active={!!historicalRange}
            onApply={(range) => setHistoricalRange(range)}
            onClear={() => setHistoricalRange(null)}
          />
        </div>
      </div>

      {/* Historical mode banner */}
      {historicalRange && (
        <div className="flex items-center justify-between px-3 py-1.5 bg-amber-900/40 border-b border-amber-700/50">
          <span className="text-xs text-amber-300">
            Viewing historical data (Iceberg) &mdash;{" "}
            {new Date(historicalRange.startMs).toLocaleString()} to{" "}
            {new Date(historicalRange.endMs).toLocaleString()} (hourly candles)
          </span>
          <button
            onClick={() => setHistoricalRange(null)}
            className="text-xs text-amber-400 hover:text-white underline"
          >
            Back to live
          </button>
        </div>
      )}

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
          {fetchError && !isLoading && (
            <div className="absolute inset-0 flex items-center justify-center bg-gray-900 bg-opacity-60 z-10">
              <div className="text-center">
                <p className="text-red-400 text-sm mb-2">{fetchError}</p>
                <button
                  onClick={() => {
                    setFetchError(null);
                    setRetryCount((c) => c + 1);
                  }}
                  className="px-3 py-1 bg-blue-600 hover:bg-blue-700 text-white rounded text-xs transition-colors"
                >
                  Retry
                </button>
              </div>
            </div>
          )}
          {noData && !isLoading && !fetchError && (
            <div className="absolute inset-0 flex items-center justify-center bg-gray-900 bg-opacity-40 z-10">
              <p className="text-gray-400 text-sm">
                No data available for {symbol} @ {timeframe}
              </p>
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
          <OrderBook symbol={symbol} />
        </div>
      )}

      {activeTab === "recentTrades" && (
        <div className="flex-1 min-h-0">
          <RecentTrades symbol={symbol} />
        </div>
      )}
    </div>
  );
};

export default CandlestickChart;
