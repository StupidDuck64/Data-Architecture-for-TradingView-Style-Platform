import React, { useState, useEffect, useRef } from "react";
import { useI18n } from "../i18n";
import { fetchOrderBook } from "../services/marketDataService";
import type { OrderBookEntry } from "../types";

interface OrderBookProps {
  symbol: string;
}

interface BookState {
  asks: OrderBookEntry[];
  bids: OrderBookEntry[];
  spread: number;
}

const OrderBook: React.FC<OrderBookProps> = ({ symbol }) => {
  const { t } = useI18n();
  const [book, setBook] = useState<BookState>({ asks: [], bids: [], spread: 0 });
  const [error, setError] = useState<string | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = () => {
      fetchOrderBook(symbol)
        .then((data) => {
          if (cancelled) return;
          // Backend returns bids/asks as [[price, qty], ...]
          const parseSide = (arr: [number, number][]): OrderBookEntry[] => {
            let running = 0;
            return (arr || []).map(([p, q]) => {
              const price = parseFloat(String(p));
              const amount = parseFloat(String(q));
              running += amount;
              return {
                price,
                amount: +amount.toFixed(4),
                total: +running.toFixed(4),
              };
            });
          };
          const asks = parseSide(data.asks);
          const bids = parseSide(data.bids);
          setBook({ asks, bids, spread: data.spread || 0 });
          setError(null);
        })
        .catch(() => {
          setError(t("failedLoadOrderBook"));
        });
    };
    load();
    intervalRef.current = setInterval(load, 2000);
    return () => {
      cancelled = true;
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [symbol, t]);

  const { asks, bids, spread } = book;
  const maxAskTotal = asks.length > 0 ? asks[asks.length - 1].total : 1;
  const maxBidTotal = bids.length > 0 ? bids[bids.length - 1].total : 1;
  const spreadPct =
    asks.length > 0 && bids.length > 0
      ? (((asks[0].price - bids[0].price) / asks[0].price) * 100).toFixed(3)
      : "0";

  const f = (v: number) =>
    v.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });

  return (
    <div className="h-full flex flex-col text-xs font-mono overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 bg-gray-800 border-b border-gray-700">
        <span className="text-gray-400 font-sans font-medium text-sm">
          {t("orderBook")}
        </span>
        <span className="text-gray-500">{symbol}</span>
      </div>

      {/* Column headers */}
      <div className="flex px-3 py-1 text-gray-500 border-b border-gray-700 bg-gray-850">
        <span className="flex-1">{t("price")}</span>
        <span className="flex-1 text-right">{t("amount")}</span>
        <span className="flex-1 text-right">{t("total")}</span>
      </div>

      {error && asks.length === 0 && bids.length === 0 && (
        <div className="flex-1 flex items-center justify-center">
          <p className="text-red-400 text-xs">{error}</p>
        </div>
      )}

      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Asks (reversed so lowest ask is at bottom) */}
        <div className="flex-1 overflow-y-auto flex flex-col-reverse">
          {asks.map((ask, i) => (
            <div key={`ask-${i}`} className="flex px-3 py-0.5 relative">
              <div
                className="absolute right-0 top-0 bottom-0 bg-red-500 bg-opacity-10"
                style={{ width: `${(ask.total / maxAskTotal) * 100}%` }}
              />
              <span className="flex-1 text-red-400 relative z-10">
                {f(ask.price)}
              </span>
              <span className="flex-1 text-right text-gray-300 relative z-10">
                {ask.amount}
              </span>
              <span className="flex-1 text-right text-gray-500 relative z-10">
                {ask.total}
              </span>
            </div>
          ))}
        </div>

        {/* Spread */}
        <div className="flex items-center justify-center gap-2 py-1.5 bg-gray-800 border-y border-gray-700">
          <span className="text-gray-400">{t("spread")}:</span>
          <span className="text-white font-semibold">{spread}</span>
          <span className="text-gray-500">({spreadPct}%)</span>
        </div>

        {/* Bids */}
        <div className="flex-1 overflow-y-auto">
          {bids.map((bid, i) => (
            <div key={`bid-${i}`} className="flex px-3 py-0.5 relative">
              <div
                className="absolute right-0 top-0 bottom-0 bg-green-500 bg-opacity-10"
                style={{ width: `${(bid.total / maxBidTotal) * 100}%` }}
              />
              <span className="flex-1 text-green-400 relative z-10">
                {f(bid.price)}
              </span>
              <span className="flex-1 text-right text-gray-300 relative z-10">
                {bid.amount}
              </span>
              <span className="flex-1 text-right text-gray-500 relative z-10">
                {bid.total}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default OrderBook;
