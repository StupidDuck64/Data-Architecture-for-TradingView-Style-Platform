import React, { useState, useCallback, useEffect, useRef } from "react";
import CandlestickChart from "./components/CandlestickChart";
import DrawingToolbar from "./components/DrawingToolbar";
import ChartOverlay from "./components/ChartOverlay";
import Header from "./components/Header";
import Watchlist from "./components/Watchlist";
import OverviewChart from "./components/OverviewChart";
import { DEFAULT_TOOL_SETTINGS, type ToolSettings } from "./components/ToolSettingsPopup";
import { fetchTickers, fetchSymbols } from "./services/marketDataService";
import { loadFromStorage, saveToStorage } from "./utils/storageHelpers";
import { useI18n } from "./i18n";
import type { Candle, Drawing, SymbolInfo, Ticker, WatchlistFilter } from "./types";

const FALLBACK_SYMBOLS = [
  "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
  "XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT",
];

interface WatchlistItemData {
  symbol: string;
  price: number;
  change: number;
  color: "green" | "red" | "gray";
}

function buildWatchlist(symbolNames: string[]): WatchlistItemData[] {
  return symbolNames.map((s) => ({
    symbol: s,
    price: 0,
    change: 0,
    color: "gray" as const,
  }));
}

const TradingDashboard: React.FC = () => {
  const { t } = useI18n();
  const [activeTool, setActiveTool] = useState("cursor");
  const [drawings, setDrawings] = useState<Drawing[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState<string>(() => {
    const stored = loadFromStorage("app_selectedSymbol", "BTCUSDT");
    if (stored && !stored.endsWith("USDT")) return "BTCUSDT";
    return stored;
  });
  const [toolSettings, setToolSettings] = useState<Record<string, ToolSettings>>(() =>
    loadFromStorage(
      "app_toolSettings",
      JSON.parse(JSON.stringify(DEFAULT_TOOL_SETTINGS)),
    ),
  );
  const [starredSymbols, setStarredSymbols] = useState<string[]>(() =>
    loadFromStorage("app_starred", []),
  );
  const [watchlistFilter, setWatchlistFilter] = useState<WatchlistFilter>("all");
  const [symbols, setSymbols] = useState<string[]>(FALLBACK_SYMBOLS);
  const [watchlistItems, setWatchlistItems] = useState<WatchlistItemData[]>(() =>
    buildWatchlist(FALLBACK_SYMBOLS),
  );
  const [showNavDrawer, setShowNavDrawer] = useState(false);
  const [connError, setConnError] = useState(false);

  // Load available symbols from backend on mount
  useEffect(() => {
    fetchSymbols()
      .then((list: SymbolInfo[]) => {
        const names = list.map((s) => s.symbol);
        if (names.length > 0) {
          setSymbols(names);
          setWatchlistItems(buildWatchlist(names));
        }
        setConnError(false);
      })
      .catch(() => {
        setConnError(true);
      });
  }, []);

  // Fetch live ticker prices for the watchlist
  useEffect(() => {
    let cancelled = false;
    const refresh = () => {
      fetchTickers()
        .then((tickers: Ticker[]) => {
          if (cancelled) return;
          const map: Record<string, Ticker> = {};
          tickers.forEach((tk) => {
            map[tk.symbol] = tk;
          });
          setWatchlistItems((prev) =>
            prev.map((item) => {
              const tick = map[item.symbol];
              if (!tick) return item;
              return {
                ...item,
                price: tick.price,
                change: tick.change24h != null ? tick.change24h : item.change,
                color:
                  tick.price > 0
                    ? (tick.change24h ?? 0) >= 0
                      ? "green"
                      : "red"
                    : item.color,
              };
            }),
          );
        })
        .catch(() => {
          if (!cancelled) setConnError(true);
        });
    };
    refresh();
    const id = setInterval(refresh, 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Persist settings to localStorage
  useEffect(() => { saveToStorage("app_toolSettings", toolSettings); }, [toolSettings]);
  useEffect(() => { saveToStorage("app_starred", starredSymbols); }, [starredSymbols]);
  useEffect(() => { saveToStorage("app_selectedSymbol", selectedSymbol); }, [selectedSymbol]);

  const handleAddDrawing = useCallback(
    (d: Drawing) => setDrawings((prev) => [...prev, d]),
    [],
  );
  const handleClearAll = useCallback(() => {
    setDrawings([]);
    setActiveTool("cursor");
  }, []);
  const handleSymbolSelect = useCallback((symbol: string) => {
    setSelectedSymbol(symbol);
    setDrawings([]);
  }, []);
  const handleToolSettingsChange = useCallback((toolId: string, newSettings: ToolSettings) => {
    setToolSettings((prev) => ({ ...prev, [toolId]: newSettings }));
  }, []);

  const handleToggleStar = useCallback((symbol: string) => {
    setStarredSymbols((prev) =>
      prev.includes(symbol)
        ? prev.filter((s) => s !== symbol)
        : [...prev, symbol],
    );
  }, []);

  // State lifted from CandlestickChart for Overview + DrawingToolbar gating
  const [chartActiveTab, setChartActiveTab] = useState("chart");
  const [chartCandles, setChartCandles] = useState<Candle[]>([]);

  // Resizable right sidebar
  const SIDEBAR_MIN = 280;
  const SIDEBAR_MAX = 520;
  const SIDEBAR_DEFAULT = 340;
  const [sidebarWidth, setSidebarWidth] = useState(SIDEBAR_DEFAULT);
  const dragging = useRef(false);

  const onDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    const onMove = (ev: MouseEvent) => {
      if (!dragging.current) return;
      const newW = window.innerWidth - ev.clientX;
      setSidebarWidth(Math.max(SIDEBAR_MIN, Math.min(SIDEBAR_MAX, newW)));
    };
    const onUp = () => {
      dragging.current = false;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, []);

  const isChartTab = chartActiveTab === "chart";

  return (
    <div className="bg-gray-900 text-white h-screen font-sans flex flex-col overflow-hidden">
      <Header showNavDrawer={showNavDrawer} onToggleDrawer={setShowNavDrawer} />

      {connError && (
        <div className="px-4 py-2 bg-red-900/50 border-b border-red-700/50 flex items-center justify-between">
          <span className="text-xs text-red-300">
            {t("connectionError")}
          </span>
          <button
            onClick={() => {
              setConnError(false);
              fetchSymbols()
                .then((list: SymbolInfo[]) => {
                  const names = list.map((s) => s.symbol);
                  if (names.length > 0) {
                    setSymbols(names);
                    setWatchlistItems(buildWatchlist(names));
                  }
                })
                .catch(() => setConnError(true));
            }}
            className="text-xs text-red-300 hover:text-white underline ml-4"
          >
            {t("retry")}
          </button>
        </div>
      )}

      <main
        className="flex-grow overflow-hidden flex"
        style={{ padding: "12px 16px" }}
      >
        {/* Drawing Toolbar — only visible on Chart tab */}
        {isChartTab && (
          <div className="mr-2 flex-shrink-0">
            <DrawingToolbar
              activeTool={activeTool}
              onToolChange={setActiveTool}
              onClearAll={handleClearAll}
              toolSettings={toolSettings}
              onToolSettingsChange={handleToolSettingsChange}
            />
          </div>
        )}

        {/* Chart area */}
        <div className="flex-grow flex flex-col" style={{ minWidth: 0 }}>
          <div
            className="bg-gray-900 rounded-lg shadow-lg flex-grow"
            style={{ minHeight: 0 }}
          >
            <CandlestickChart
              symbol={selectedSymbol}
              symbols={symbols}
              starredSymbols={starredSymbols}
              onToggleStar={handleToggleStar}
              onSymbolChange={handleSymbolSelect}
              onActiveTabChange={setChartActiveTab}
              onCandlesChange={setChartCandles}
            >
              <ChartOverlay
                activeTool={isChartTab ? activeTool : "cursor"}
                drawings={drawings}
                onAddDrawing={handleAddDrawing}
                toolSettings={toolSettings}
              />
            </CandlestickChart>
          </div>
        </div>

        {/* Drag handle */}
        <div
          onMouseDown={onDragStart}
          className="flex-shrink-0 cursor-col-resize flex items-center justify-center group"
          style={{ width: 6, margin: "0 2px" }}
        >
          <div className="w-[3px] h-10 rounded-full bg-gray-700 group-hover:bg-blue-500 transition-colors" />
        </div>

        {/* Right sidebar: Watchlist + Overview */}
        <aside
          className="flex-shrink-0 flex flex-col gap-2 overflow-hidden"
          style={{ width: sidebarWidth }}
        >
          <div className="min-h-0" style={{ flex: 6.5 }}>
            <Watchlist
              items={watchlistItems}
              selectedSymbol={selectedSymbol}
              starredSymbols={starredSymbols}
              filter={watchlistFilter}
              onFilterChange={setWatchlistFilter}
              onSymbolSelect={handleSymbolSelect}
              onToggleStar={handleToggleStar}
            />
          </div>
          <div
            className="min-h-0 bg-gray-800 rounded-lg overflow-y-auto"
            style={{ flex: 3.5 }}
          >
            <OverviewChart symbol={selectedSymbol} candles={chartCandles} />
          </div>
        </aside>
      </main>
    </div>
  );
};

export default TradingDashboard;
