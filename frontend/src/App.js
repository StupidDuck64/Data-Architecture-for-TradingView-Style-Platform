import React, { useState, useCallback, useEffect, useRef } from "react";
import { Search, Menu, User, LogOut, Star, X } from "lucide-react";
import CandlestickChart from "./components/CandlestickChart";
import DrawingToolbar from "./components/DrawingToolbar";
import ChartOverlay from "./components/ChartOverlay";
import AuthModal from "./components/AuthModal";
import LanguageSwitcher from "./components/LanguageSwitcher";
import { DEFAULT_TOOL_SETTINGS } from "./components/ToolSettingsPopup";
import { useI18n } from "./i18n";
import { useAuth } from "./contexts/AuthContext";

const watchlistItems = [
  { symbol: "BTCUSDT", price: 64444, change: 0.33, color: "green" },
  { symbol: "ETHUSDT", price: 3400, change: -0.53, color: "red" },
  { symbol: "BNBUSDT", price: 580, change: -0.27, color: "red" },
  { symbol: "SOLUSDT", price: 165, change: 0.54, color: "green" },
  { symbol: "XRPUSDT", price: 2.35, change: -0.16, color: "red" },
  { symbol: "DOGEUSDT", price: 0.158, change: -0.6, color: "red" },
  { symbol: "ADAUSDT", price: 0.72, change: -0.83, color: "red" },
  { symbol: "AVAXUSDT", price: 35.2, change: 0.33, color: "green" },
];

// LocalStorage helpers for user customization
function loadFromStorage(key, fallback) {
  try {
    const v = localStorage.getItem(key);
    return v ? JSON.parse(v) : fallback;
  } catch {
    return fallback;
  }
}
function saveToStorage(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {}
}

const TradingDashboard = () => {
  const { t } = useI18n();
  const { user, logout } = useAuth();
  const [activeTool, setActiveTool] = useState("cursor");
  const [drawings, setDrawings] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState(() => {
    const stored = loadFromStorage("app_selectedSymbol", "BTCUSDT");
    // Migrate legacy symbol names (e.g. BTCUSD → BTCUSDT)
    if (stored && !stored.endsWith("USDT")) return "BTCUSDT";
    return stored;
  });
  const [toolSettings, setToolSettings] = useState(() =>
    loadFromStorage(
      "app_toolSettings",
      JSON.parse(JSON.stringify(DEFAULT_TOOL_SETTINGS)),
    ),
  );
  const [starredSymbols, setStarredSymbols] = useState(() =>
    loadFromStorage("app_starred", []),
  );
  const [showAuthModal, setShowAuthModal] = useState(false);
  const [watchlistFilter, setWatchlistFilter] = useState("all"); // 'all' | 'starred'
  const [showNavDrawer, setShowNavDrawer] = useState(false);
  const drawerRef = useRef(null);

  // Persist settings to localStorage
  useEffect(() => {
    saveToStorage("app_toolSettings", toolSettings);
  }, [toolSettings]);
  useEffect(() => {
    saveToStorage("app_starred", starredSymbols);
  }, [starredSymbols]);
  useEffect(() => {
    saveToStorage("app_selectedSymbol", selectedSymbol);
  }, [selectedSymbol]);

  const handleAddDrawing = useCallback(
    (d) => setDrawings((prev) => [...prev, d]),
    [],
  );
  const handleClearAll = useCallback(() => {
    setDrawings([]);
    setActiveTool("cursor");
  }, []);
  const handleSymbolSelect = useCallback((symbol) => {
    setSelectedSymbol(symbol);
    setDrawings([]);
  }, []);
  const handleToolSettingsChange = useCallback((toolId, newSettings) => {
    setToolSettings((prev) => ({ ...prev, [toolId]: newSettings }));
  }, []);

  const handleToggleStar = useCallback((symbol) => {
    setStarredSymbols((prev) =>
      prev.includes(symbol)
        ? prev.filter((s) => s !== symbol)
        : [...prev, symbol],
    );
  }, []);

  // Close drawer on outside click
  useEffect(() => {
    const handleClick = (e) => {
      if (drawerRef.current && !drawerRef.current.contains(e.target)) {
        setShowNavDrawer(false);
      }
    };
    if (showNavDrawer) document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [showNavDrawer]);

  const filteredWatchlist =
    watchlistFilter === "starred"
      ? watchlistItems.filter((item) => starredSymbols.includes(item.symbol))
      : watchlistItems;

  const navItems = [
    { key: "products", label: t("products") },
    { key: "community", label: t("community") },
    { key: "markets", label: t("markets") },
    { key: "news", label: t("news") },
    { key: "brokers", label: t("brokers") },
  ];

  return (
    <div className="bg-gray-900 text-white min-h-screen font-sans flex flex-col">
      <header className="bg-gray-800 p-4 flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <span className="text-2xl font-bold text-blue-500">
            Crypto Dashboard
          </span>
        </div>
        <div className="flex items-center space-x-3">
          <div className="relative">
            <input
              type="text"
              placeholder={t("search")}
              className="bg-gray-700 text-white rounded-full py-2 px-4 pl-10 w-40 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all duration-200"
            />
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
          </div>

          <LanguageSwitcher />

          {/* Auth area */}
          {user ? (
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-300">
                <User size={14} className="inline mr-1" />
                {user.name}
              </span>
              <button
                onClick={logout}
                className="flex items-center gap-1 text-sm text-gray-400 hover:text-white px-2 py-1 rounded hover:bg-gray-700 transition-colors"
                title={t("logout")}
              >
                <LogOut size={14} />
              </button>
            </div>
          ) : (
            <button
              onClick={() => setShowAuthModal(true)}
              className="bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium px-4 py-1.5 rounded-full transition-colors"
            >
              {t("login")}
            </button>
          )}

          <button
            onClick={() => setShowNavDrawer(true)}
            className="bg-blue-600 rounded-full p-2 hover:bg-blue-700 transition-colors duration-200"
          >
            <Menu className="w-5 h-5" />
          </button>
        </div>
      </header>

      {/* Navigation Drawer overlay */}
      {showNavDrawer && (
        <div className="fixed inset-0 bg-black bg-opacity-50 z-[300]">
          <div
            ref={drawerRef}
            className="absolute right-0 top-0 h-full w-64 bg-gray-800 shadow-2xl flex flex-col"
          >
            <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
              <span className="text-lg font-bold text-blue-500">
                Crypto Dashboard
              </span>
              <button
                onClick={() => setShowNavDrawer(false)}
                className="text-gray-400 hover:text-white transition-colors"
              >
                <X size={20} />
              </button>
            </div>
            <nav className="flex flex-col p-4 space-y-1">
              {navItems.map((item) => (
                <a
                  key={item.key}
                  href="#"
                  onClick={() => setShowNavDrawer(false)}
                  className="px-4 py-2.5 rounded-lg text-gray-300 hover:text-white hover:bg-gray-700 transition-colors duration-150 font-medium"
                >
                  {item.label}
                </a>
              ))}
            </nav>
          </div>
        </div>
      )}

      <main className="flex-grow p-6 overflow-hidden flex">
        {/* Drawing Toolbar */}
        <div className="mr-3 flex-shrink-0">
          <DrawingToolbar
            activeTool={activeTool}
            onToolChange={setActiveTool}
            onClearAll={handleClearAll}
            toolSettings={toolSettings}
            onToolSettingsChange={handleToolSettingsChange}
          />
        </div>

        <div className="flex-grow mr-4 flex flex-col">
          <div
            className="bg-gray-900 rounded-lg shadow-lg flex-grow"
            style={{ minHeight: 0 }}
          >
            <CandlestickChart
              symbol={selectedSymbol}
              starredSymbols={starredSymbols}
              onToggleStar={handleToggleStar}
              onSymbolChange={handleSymbolSelect}
            >
              <ChartOverlay
                activeTool={activeTool}
                drawings={drawings}
                onAddDrawing={handleAddDrawing}
                toolSettings={toolSettings}
              />
            </CandlestickChart>
          </div>
        </div>

        <aside className="w-64 bg-gray-800 p-4 rounded-lg overflow-y-auto">
          <h3 className="text-xl font-semibold mb-2 text-blue-400">
            {t("watchlist")}
          </h3>
          {/* Watchlist filter tabs */}
          <div className="flex gap-1 mb-3">
            <button
              onClick={() => setWatchlistFilter("all")}
              className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
                watchlistFilter === "all"
                  ? "bg-blue-600 text-white"
                  : "text-gray-400 hover:text-white hover:bg-gray-700"
              }`}
            >
              {t("all")}
            </button>
            <button
              onClick={() => setWatchlistFilter("starred")}
              className={`flex items-center gap-1 px-3 py-1 rounded text-xs font-medium transition-colors ${
                watchlistFilter === "starred"
                  ? "bg-blue-600 text-white"
                  : "text-gray-400 hover:text-white hover:bg-gray-700"
              }`}
            >
              <Star size={10} /> {t("starred")}
            </button>
          </div>
          <ul className="space-y-2">
            {filteredWatchlist.map((item) => {
              const isActive = selectedSymbol === item.symbol;
              const isStarred = starredSymbols.includes(item.symbol);
              return (
                <li
                  key={item.symbol}
                  className={`flex justify-between items-center p-2 rounded-lg cursor-pointer transition-colors duration-200 ${
                    isActive
                      ? "bg-blue-700 ring-1 ring-blue-400"
                      : "bg-gray-700 hover:bg-gray-600"
                  }`}
                >
                  <div
                    className="flex-1"
                    onClick={() => handleSymbolSelect(item.symbol)}
                  >
                    <span className="font-medium">{item.symbol}</span>
                    <span className="block text-sm text-gray-400">
                      {item.price.toLocaleString()}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleToggleStar(item.symbol);
                      }}
                      className={`p-0.5 transition-colors ${isStarred ? "text-yellow-400" : "text-gray-600 hover:text-gray-400"}`}
                    >
                      <Star
                        size={14}
                        fill={isStarred ? "currentColor" : "none"}
                      />
                    </button>
                    <div
                      className={`flex items-center ${item.color === "green" ? "text-green-400" : "text-red-400"}`}
                    >
                      <span>
                        {item.change > 0 ? "+" : ""}
                        {item.change}%
                      </span>
                    </div>
                  </div>
                </li>
              );
            })}
            {filteredWatchlist.length === 0 && (
              <li className="text-center text-gray-500 text-sm py-4">
                {watchlistFilter === "starred"
                  ? "No starred symbols"
                  : "No symbols"}
              </li>
            )}
          </ul>
        </aside>
      </main>

      {/* Auth Modal */}
      <AuthModal
        isOpen={showAuthModal}
        onClose={() => setShowAuthModal(false)}
      />
    </div>
  );
};

export default TradingDashboard;
