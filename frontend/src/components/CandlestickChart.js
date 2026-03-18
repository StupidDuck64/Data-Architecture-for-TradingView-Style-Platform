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
  fetchHistoricalCandles,
  subscribeCandle,
} from "../services/marketDataService";
import MarketSelector from "./MarketSelector";
import DateRangePicker from "./DateRangePicker";
import OrderBook from "./OrderBook";
import RecentTrades from "./RecentTrades";
import {
  THEME,
  TIMEFRAMES,
  CHART_TABS,
  TAB_ICONS,
  DEFAULT_INDICATOR_SETTINGS,
  localTickMarkFormatter,
  localTimeFormatter,
} from "./chart/chartConstants";
import { calcSMA, calcEMA, calcRSI, calcMFI } from "./chart/indicatorUtils";
import IndicatorPanel from "./chart/IndicatorPanel";
import OHLCVBar from "./chart/OHLCVBar";

const CandlestickChart = ({
  defaultSymbol = "BTCUSDT",
  symbol: symbolProp,
  symbols = [],
  children,
  starredSymbols = [],
  onToggleStar,
  onSymbolChange,
  onActiveTabChange,
  onCandlesChange,
}) => {
  const { t } = useI18n();
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const candleRef = useRef(null);
  const volumeRef = useRef(null);
  const sma20Ref = useRef(null);
  const sma50Ref = useRef(null);
  const emaRef = useRef(null);
  const rsiSeriesRef = useRef(null);
  const mfiSeriesRef = useRef(null);
  const candlesRef = useRef([]);
  const symbolRef = useRef(defaultSymbol);
  const timeframeRef = useRef("1m");

  const [symbol, setSymbol] = useState(symbolProp || defaultSymbol);
  const [timeframe, setTimeframe] = useState("1m");
  const [tooltip, setTooltip] = useState(null);
  const [showIndPanel, setShowIndPanel] = useState(false);
  const [indSettings, setIndSettings] = useState(() =>
    JSON.parse(JSON.stringify(DEFAULT_INDICATOR_SETTINGS)),
  );
  const [activeTab, setActiveTab] = useState("chart");
  const [candles, setCandles] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [fetchError, setFetchError] = useState(null);
  const [retryCount, setRetryCount] = useState(0);
  const [historicalRange, setHistoricalRange] = useState(null); // { startMs, endMs } or null
  const [noData, setNoData] = useState(false);
  const isLoadingMoreRef = useRef(false);
  const earliestTimestampRef = useRef(null);
  const noMoreDataRef = useRef(false);
  const scrollCooldownRef = useRef(0);

  // Keep latest symbol/timeframe in refs so async scroll-load results can be
  // ignored when the user changes context mid-request.
  useEffect(() => {
    symbolRef.current = symbol;
    timeframeRef.current = timeframe.toLowerCase();
  }, [symbol, timeframe]);

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
        timeFormatter: localTimeFormatter,
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
        minimumWidth: 80,
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
    // RSI & MFI on a separate left-side oscillator scale (bottom 20%)
    const rsiSeries = chart.addSeries(LineSeries, {
      color: THEME.rsi,
      lineWidth: 1.5,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
      visible: false,
      priceScaleId: "oscillator",
    });
    const mfiSeries = chart.addSeries(LineSeries, {
      color: THEME.mfi,
      lineWidth: 1.5,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
      visible: false,
      priceScaleId: "oscillator",
    });
    chart.priceScale("oscillator").applyOptions({
      scaleMargins: { top: 0.75, bottom: 0 },
      visible: false,
    });
    chartRef.current = chart;
    candleRef.current = cs;
    volumeRef.current = vs;
    sma20Ref.current = s20;
    sma50Ref.current = s50;
    emaRef.current = ema;
    rsiSeriesRef.current = rsiSeries;
    mfiSeriesRef.current = mfiSeries;
    chart.subscribeCrosshairMove((param) => {
      if (!param.time || (param.point && param.point.x < 0)) {
        setTooltip(null);
        return;
      }
      const c = param.seriesData.get(cs);
      const v = param.seriesData.get(vs);
      if (c) {
        const lbl = localTimeFormatter(param.time);
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

  // Helper: load more historical data when scrolling left
  const loadMoreHistoricalData = useCallback(async () => {
    if (isLoadingMoreRef.current || noMoreDataRef.current || !candleRef.current || historicalRange) return;
    // Cooldown: prevent rapid-fire loads
    if (Date.now() - scrollCooldownRef.current < 500) return;
    const current = candlesRef.current;
    if (current.length === 0) return;

    const earliestTime = current[0].time;
    const requestSymbol = symbol;
    const requestInterval = timeframe.toLowerCase();
    
    isLoadingMoreRef.current = true;
    try {
      const limit = 500;
      const olderData = await fetchCandles(requestSymbol, requestInterval, limit, earliestTime);

      // User changed symbol/timeframe while request was in flight.
      if (
        symbolRef.current !== requestSymbol
        || timeframeRef.current !== requestInterval
      ) {
        isLoadingMoreRef.current = false;
        return;
      }
      
      if (olderData.length === 0) {
        noMoreDataRef.current = true;
        isLoadingMoreRef.current = false;
        return;
      }

      // Filter out duplicates and merge
      const newCandles = olderData.filter(c => c.time < earliestTime);
      if (newCandles.length === 0) {
        noMoreDataRef.current = true;
        isLoadingMoreRef.current = false;
        return;
      }
      const merged = [...newCandles, ...current];
      
      // Update refs BEFORE touching chart
      candlesRef.current = merged;
      earliestTimestampRef.current = merged[0].time;
      
      // Preserve visible range so chart doesn't jump
      const ts = chartRef.current ? chartRef.current.timeScale() : null;
      const visibleRange = ts ? ts.getVisibleLogicalRange() : null;

      // Apply to chart series
      candleRef.current.setData(merged);
      if (onCandlesChange) onCandlesChange(merged);

      // Restore visible range offset (older data shifts indices)
      if (ts && visibleRange) {
        const shift = newCandles.length;
        ts.setVisibleLogicalRange({
          from: visibleRange.from + shift,
          to: visibleRange.to + shift,
        });
      }
      
      // Update volume + indicators
      const vs = volumeRef.current;
      if (vs) {
        vs.setData(
          merged.map((c) => ({
            time: c.time,
            value: c.volume,
            color: c.close >= c.open ? THEME.volumeUp : THEME.volumeDown,
          })),
        );
      }
      if (sma20Ref.current)
        sma20Ref.current.setData(calcSMA(merged, indSettings.sma20.period));
      if (sma50Ref.current)
        sma50Ref.current.setData(calcSMA(merged, indSettings.sma50.period));
      if (emaRef.current)
        emaRef.current.setData(calcEMA(merged, indSettings.ema.period));
      if (rsiSeriesRef.current)
        rsiSeriesRef.current.setData(calcRSI(merged, indSettings.rsi.period));
      if (mfiSeriesRef.current)
        mfiSeriesRef.current.setData(calcMFI(merged, indSettings.mfi.period));

      // Update React state last (avoid triggering re-renders mid-update)
      setCandles(merged);
      scrollCooldownRef.current = Date.now();
      isLoadingMoreRef.current = false;
    } catch (error) {
      console.error('Failed to load more historical data:', error);
      isLoadingMoreRef.current = false;
    }
  }, [symbol, timeframe, historicalRange, indSettings, onCandlesChange]);

  // Subscribe to scroll/zoom events to load more historical data
  useEffect(() => {
    if (!chartRef.current || historicalRange) return;
    
    const timeScale = chartRef.current.timeScale();
    const handleVisibleRangeChange = () => {
      const logicalRange = timeScale.getVisibleLogicalRange();
      if (!logicalRange) return;
      
      // If user scrolls close to the left edge, load more data
      if (logicalRange.from < 20) {
        loadMoreHistoricalData();
      }
    };
    
    timeScale.subscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
    
    return () => {
      timeScale.unsubscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
    };
  }, [loadMoreHistoricalData, historicalRange]);

  // Helper: push OHLCV data into chart series + indicators
  const applyDataToChart = useCallback(
    (data) => {
      if (!candleRef.current) return;
      setCandles(data);
      candlesRef.current = data;
      if (data.length > 0) {
        earliestTimestampRef.current = data[0].time;
        noMoreDataRef.current = false;
        scrollCooldownRef.current = 0;
      }
      if (onCandlesChange) onCandlesChange(data);
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
      if (rsiSeriesRef.current)
        rsiSeriesRef.current.setData(calcRSI(data, indSettings.rsi.period));
      if (mfiSeriesRef.current)
        mfiSeriesRef.current.setData(calcMFI(data, indSettings.mfi.period));
      if (chartRef.current) chartRef.current.timeScale().fitContent();
      if (data.length > 0)
        setTooltip({ ...data[data.length - 1], timeLabel: "" });
    },
    [indSettings, onCandlesChange],
  );

  // Load data when symbol or timeframe changes (live mode) + auto-refresh
  useEffect(() => {
    if (!candleRef.current || historicalRange) return;
    let cancelled = false;
    const is1s = timeframe === "1s";

    // Update secondsVisible based on timeframe
    if (chartRef.current) {
      const showSeconds = is1s || timeframe === "1m";
      chartRef.current
        .timeScale()
        .applyOptions({ secondsVisible: showSeconds });
    }

    const limit = is1s ? 120 : 200;
    const maxBars = is1s ? 120 : 10000;

    // Full load — fetches candles, rebuilds all series + indicators
    const loadData = () => {
      setFetchError(null);
      return fetchCandles(symbol, timeframe.toLowerCase(), limit)
        .then((data) => {
          if (cancelled) return;
          applyDataToChart(data);
          setIsLoading(false);
        })
        .catch(() => {
          if (cancelled) return;
          setIsLoading(false);
          setFetchError("Failed to load candle data");
        });
    };

    setIsLoading(true);
    setFetchError(null);
    setNoData(false);
    loadData();

    // Subscribe to real-time candle updates via WebSocket.
    //
    // 1s  : Every WS message with a new timestamp draws a brand-new candle.
    //        Same-second repeats are ignored (candle is already drawn).
    //
    // 1m+ : Same openTime → update (agitate) the latest bar in real-time.
    //        New openTime  → finalize previous bar, start a new one.
    const unsub = subscribeCandle(
      symbol,
      timeframe.toLowerCase(),
      (candle) => {
        if (cancelled || !candleRef.current) return;
        const prev = candlesRef.current;
        if (prev.length === 0) return;

        const lastTime = prev[prev.length - 1].time;

        if (is1s) {
          // ── 1-second mode: only draw new candles, never update old ones ──
          if (candle.time <= lastTime) return; // same or stale second → skip
          candleRef.current.update(candle);
          if (volumeRef.current) {
            volumeRef.current.update({
              time: candle.time,
              value: candle.volume,
              color:
                candle.close >= candle.open
                  ? THEME.volumeUp
                  : THEME.volumeDown,
            });
          }
          const next = [...prev.slice(-(maxBars - 1)), candle];
          candlesRef.current = next;
          setCandles(next);
          if (onCandlesChange) onCandlesChange(next);
          setTooltip((tip) =>
            tip ? { ...tip, ...candle, timeLabel: tip.timeLabel } : null,
          );
        } else {
          // ── Other timeframes: agitate latest bar, append on new period ──
          if (candle.time < lastTime) return; // stale → skip
          candleRef.current.update(candle);
          if (volumeRef.current) {
            volumeRef.current.update({
              time: candle.time,
              value: candle.volume,
              color:
                candle.close >= candle.open
                  ? THEME.volumeUp
                  : THEME.volumeDown,
            });
          }
          let next;
          if (candle.time === lastTime) {
            // Same period → replace last element
            next = [...prev];
            next[next.length - 1] = candle;
          } else {
            // New period → append
            next = [...prev.slice(-(maxBars - 1)), candle];
          }
          candlesRef.current = next;
          setCandles(next);
          if (onCandlesChange) onCandlesChange(next);
          setTooltip((tip) =>
            tip ? { ...tip, ...candle, timeLabel: tip.timeLabel } : null,
          );
        }
      },
    );

    // Incremental poll: fetch only the last few candles and merge
    // instead of full-reload which causes chart flickering.
    const pollIncremental = () => {
      if (cancelled || !candleRef.current) return;
      const fetchLimit = is1s ? 5 : 3;
      fetchCandles(symbol, timeframe.toLowerCase(), fetchLimit)
        .then((data) => {
          if (cancelled || !candleRef.current || data.length === 0) return;
          const prev = candlesRef.current;
          if (prev.length === 0) {
            // No data yet, do a full load
            applyDataToChart(data);
            return;
          }
          let changed = false;
          for (const c of data) {
            const existIdx = prev.findIndex((p) => p.time === c.time);
            if (existIdx >= 0) {
              // Skip poll updates for the live (latest) candle on >=1m — WS is authoritative
              if (!is1s && existIdx === prev.length - 1) continue;
              const old = prev[existIdx];
              if (old.close !== c.close || old.high !== c.high || old.low !== c.low || old.volume !== c.volume) {
                prev[existIdx] = c;
                candleRef.current.update(c);
                if (volumeRef.current) {
                  volumeRef.current.update({
                    time: c.time,
                    value: c.volume,
                    color: c.close >= c.open ? THEME.volumeUp : THEME.volumeDown,
                  });
                }
                changed = true;
              }
            } else if (c.time > prev[prev.length - 1].time) {
              if (is1s) {
                prev.push(c);
                while (prev.length > maxBars) prev.shift();
              } else {
                prev.push(c);
                while (prev.length > maxBars) prev.shift();
              }
              candleRef.current.update(c);
              if (volumeRef.current) {
                volumeRef.current.update({
                  time: c.time,
                  value: c.volume,
                  color: c.close >= c.open ? THEME.volumeUp : THEME.volumeDown,
                });
              }
              changed = true;
            }
          }
          if (changed) {
            candlesRef.current = [...prev];
            setCandles([...prev]);
            if (onCandlesChange) onCandlesChange([...prev]);
          }
        })
        .catch(() => {});
    };

    const pollInterval = is1s ? 1500 : 3000;
    const pollId = setInterval(pollIncremental, pollInterval);

    return () => {
      cancelled = true;
      if (pollId) clearInterval(pollId);
      if (unsub) unsub();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol, timeframe, historicalRange, retryCount]);

  // Load historical data from Iceberg when date range is set
  useEffect(() => {
    if (!candleRef.current || !historicalRange) return;
    let cancelled = false;
    setIsLoading(true);
    setFetchError(null);
    const interval = timeframe.toLowerCase();
    fetchHistoricalCandles(
      symbol,
      historicalRange.startMs,
      historicalRange.endMs,
      2000,
      interval,
    )
      .then(async (data) => {
        if (cancelled) return;

        if (data.length > 0) {
          applyDataToChart(data);
          setIsLoading(false);
          return;
        }

        // If low-timeframe cold data is unavailable for this symbol/range,
        // gracefully fall back to 1H so historical mode still renders.
        if (["1m", "5m", "15m"].includes(interval)) {
          try {
            const fallback = await fetchHistoricalCandles(
              symbol,
              historicalRange.startMs,
              historicalRange.endMs,
              2000,
              "1h",
            );
            if (cancelled) return;

            if (fallback.length > 0) {
              applyDataToChart(fallback);
              if (timeframe !== "1H") {
                setTimeframe("1H");
              }
              setFetchError(null);
              setIsLoading(false);
              return;
            }
          } catch (_) {
            // Fall through to unified no-data message.
          }
        }

        applyDataToChart([]);
        setIsLoading(false);
        setFetchError("No historical data for this range");
      })
      .catch(() => {
        if (cancelled) return;
        setIsLoading(false);
        setFetchError("Failed to load historical data");
      });

    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol, historicalRange, timeframe, retryCount]);

  // Re-layout chart when chart tab becomes visible again
  useEffect(() => {
    if (onActiveTabChange) onActiveTabChange(activeTab);
    if (activeTab === "chart" && chartRef.current && containerRef.current) {
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
    // RSI / MFI on main chart oscillator scale
    const cfgR = indSettings.rsi;
    const cfgM = indSettings.mfi;
    if (rsiSeriesRef.current) {
      rsiSeriesRef.current.applyOptions({
        visible: cfgR.visible,
        color: cfgR.color,
        lineWidth: cfgR.lineWidth || 1.5,
      });
      rsiSeriesRef.current.setData(calcRSI(candles, cfgR.period));
    }
    if (mfiSeriesRef.current) {
      mfiSeriesRef.current.applyOptions({
        visible: cfgM.visible,
        color: cfgM.color,
        lineWidth: cfgM.lineWidth || 1.5,
      });
      mfiSeriesRef.current.setData(calcMFI(candles, cfgM.period));
    }
    // Show/hide the oscillator price scale when either is visible
    if (chartRef.current) {
      chartRef.current
        .priceScale("oscillator")
        .applyOptions({ visible: cfgR.visible || cfgM.visible });
    }
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

  const historicalTimeframes = TIMEFRAMES.filter((tf) => tf !== "1s");

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
          {(historicalRange ? historicalTimeframes : TIMEFRAMES).map((tf) => (
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
          {/* Historical date-range picker */}
          <DateRangePicker
            active={!!historicalRange}
            onApply={(range) => {
              if (timeframe === "1s") setTimeframe("1m");
              setHistoricalRange(range);
            }}
            onClear={() => setHistoricalRange(null)}
          />
        </div>
      </div>

      {/* Historical mode banner */}
      {historicalRange && (
        <div className="flex items-center justify-between px-3 py-1.5 bg-amber-900/40 border-b border-amber-700/50">
          <span className="text-xs text-amber-300">
            Viewing historical data &mdash;{" "}
            {new Date(historicalRange.startMs).toLocaleString()} to{" "}
            {new Date(historicalRange.endMs).toLocaleString()} ({timeframe} candles)
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
      <div style={{ display: activeTab === "chart" ? "contents" : "none" }}>
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
      </div>

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
