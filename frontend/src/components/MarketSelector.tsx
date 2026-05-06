import React, { useState, useRef, useEffect } from "react";
import { Search, ChevronDown, Star } from "lucide-react";
import { useI18n } from "../i18n";
import { useSymbolMeta } from "../hooks/useSymbolMeta";

interface MarketSelectorProps {
  symbols: string[];
  selectedSymbol: string;
  onSelect: (symbol: string) => void;
  starredSymbols: string[];
  onToggleStar: (symbol: string) => void;
}

const MarketSelector: React.FC<MarketSelectorProps> = ({
  symbols,
  selectedSymbol,
  onSelect,
  starredSymbols,
  onToggleStar,
}) => {
  const { t } = useI18n();
  const { getMeta } = useSymbolMeta();
  const [isOpen, setIsOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target as Node)
      ) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const filtered = symbols.filter((s) => {
    const meta = getMeta(s);
    const matchesSearch =
      !searchQuery ||
      s.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (meta?.name &&
        meta.name.toLowerCase().includes(searchQuery.toLowerCase()));
    return matchesSearch;
  });

  const selectedMeta = getMeta(selectedSymbol);

  return (
    <div ref={dropdownRef} className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-2 bg-gray-700 text-white text-sm rounded px-3 py-1.5 border border-gray-600 hover:border-gray-500 focus:outline-none focus:border-blue-500 cursor-pointer min-w-[160px]"
      >
        {selectedMeta?.logoUrl ? (
          <img
            src={selectedMeta.logoUrl}
            alt={selectedMeta.name}
            className="w-5 h-5 rounded-full"
            onError={(e) => {
              (e.target as HTMLImageElement).style.display = "none";
            }}
          />
        ) : (
          <span className="w-5 h-5 rounded-full bg-gray-600 flex items-center justify-center text-[10px] text-gray-300">
            {selectedSymbol.charAt(0)}
          </span>
        )}
        <span className="font-medium">{selectedSymbol}</span>
        <ChevronDown size={14} className="ml-auto text-gray-400" />
      </button>

      {isOpen && (
        <div className="absolute top-full left-0 mt-1 w-72 bg-gray-800 border border-gray-700 rounded-lg shadow-2xl z-[150] overflow-hidden">
          {/* Search */}
          <div className="p-2 border-b border-gray-700">
            <div className="relative">
              <Search
                size={14}
                className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400"
              />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder={t("searchSymbol")}
                className="w-full bg-gray-700 text-white text-sm rounded px-3 py-1.5 pl-8 border border-gray-600 focus:outline-none focus:border-blue-500"
                autoFocus
              />
            </div>
          </div>

          {/* Symbol list */}
          <div className="max-h-64 overflow-y-auto">
            {filtered.length === 0 && (
              <div className="px-3 py-4 text-center text-gray-500 text-sm">
                {t("noResults")}
              </div>
            )}
            {filtered.map((s) => {
              const m = getMeta(s);
              const isActive = s === selectedSymbol;
              const isStarred = starredSymbols.includes(s);
              return (
                <div
                  key={s}
                  className={`flex items-center gap-2 px-3 py-2 cursor-pointer transition-colors ${
                    isActive
                      ? "bg-blue-600 bg-opacity-30"
                      : "hover:bg-gray-700"
                  }`}
                  onClick={() => {
                    onSelect(s);
                    setIsOpen(false);
                    setSearchQuery("");
                  }}
                >
                  {m?.logoUrl ? (
                    <img
                      src={m.logoUrl}
                      alt={m.name}
                      className="w-5 h-5 rounded-full flex-shrink-0"
                      onError={(e) => {
                        (e.target as HTMLImageElement).style.display = "none";
                      }}
                    />
                  ) : (
                    <span className="w-5 h-5 rounded-full bg-gray-600 flex items-center justify-center text-[10px] text-gray-300 flex-shrink-0">
                      {s.charAt(0)}
                    </span>
                  )}
                  <div className="flex-1 min-w-0">
                    <span className="text-sm font-medium text-white">{s}</span>
                    {m?.name && (
                      <span className="text-xs text-gray-400 ml-2">
                        {m.name}
                      </span>
                    )}
                  </div>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      onToggleStar(s);
                    }}
                    className={`p-0.5 rounded transition-colors ${isStarred ? "text-yellow-400" : "text-gray-600 hover:text-gray-400"}`}
                  >
                    <Star
                      size={14}
                      fill={isStarred ? "currentColor" : "none"}
                    />
                  </button>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

export default MarketSelector;
