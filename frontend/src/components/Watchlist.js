import React, { useState } from "react";
import { Star, ChevronLeft, ChevronRight } from "lucide-react";
import { useI18n } from "../i18n";

const PAGE_SIZE = 10;

const Watchlist = ({
  items,
  selectedSymbol,
  starredSymbols,
  filter,
  onFilterChange,
  onSymbolSelect,
  onToggleStar,
}) => {
  const { t } = useI18n();
  const [page, setPage] = useState(0);

  const allFiltered =
    filter === "starred"
      ? items.filter((item) => starredSymbols.includes(item.symbol))
      : items;
  const totalPages = Math.max(1, Math.ceil(allFiltered.length / PAGE_SIZE));
  // Reset page if it overflows after filter change
  const safePage = Math.min(page, totalPages - 1);
  const filteredItems = allFiltered.slice(
    safePage * PAGE_SIZE,
    (safePage + 1) * PAGE_SIZE,
  );

  return (
    <aside className="w-64 bg-gray-800 p-4 rounded-lg overflow-y-auto">
      <h3 className="text-xl font-semibold mb-2 text-blue-400">
        {t("watchlist")}
      </h3>
      <div className="flex gap-1 mb-3">
        <button
          onClick={() => onFilterChange("all")}
          className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
            filter === "all"
              ? "bg-blue-600 text-white"
              : "text-gray-400 hover:text-white hover:bg-gray-700"
          }`}
        >
          {t("all")}
        </button>
        <button
          onClick={() => onFilterChange("starred")}
          className={`flex items-center gap-1 px-3 py-1 rounded text-xs font-medium transition-colors ${
            filter === "starred"
              ? "bg-blue-600 text-white"
              : "text-gray-400 hover:text-white hover:bg-gray-700"
          }`}
        >
          <Star size={10} /> {t("starred")}
        </button>
      </div>
      <ul className="space-y-2">
        {filteredItems.map((item) => {
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
                onClick={() => onSymbolSelect(item.symbol)}
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
                    onToggleStar(item.symbol);
                  }}
                  className={`p-0.5 transition-colors ${isStarred ? "text-yellow-400" : "text-gray-600 hover:text-gray-400"}`}
                >
                  <Star size={14} fill={isStarred ? "currentColor" : "none"} />
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
        {filteredItems.length === 0 && (
          <li className="text-center text-gray-500 text-sm py-4">
            {filter === "starred" ? "No starred symbols" : "No symbols"}
          </li>
        )}
      </ul>
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-2">
          <button
            onClick={() => setPage((p) => Math.max(0, p - 1))}
            disabled={safePage === 0}
            className="p-1 rounded text-gray-400 hover:text-white disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
          >
            <ChevronLeft size={16} />
          </button>
          <span className="text-xs text-gray-500">
            {safePage + 1} / {totalPages}
          </span>
          <button
            onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
            disabled={safePage >= totalPages - 1}
            className="p-1 rounded text-gray-400 hover:text-white disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
          >
            <ChevronRight size={16} />
          </button>
        </div>
      )}
    </aside>
  );
};

export default Watchlist;
