import { useState, useEffect, useCallback, useRef } from "react";
import {
  getSymbolMetadata,
  lookupSymbol,
  type SymbolMetaEntry,
} from "../services/symbolMetaService";

interface UseSymbolMetaResult {
  /** Look up metadata for a Binance pair like "BTCUSDT" */
  getMeta: (symbol: string) => SymbolMetaEntry | null;
  /** Whether the initial load is in progress */
  isLoading: boolean;
}

/**
 * Hook that loads symbol metadata (logos, names) on mount.
 * - Serves from localStorage cache instantly if available.
 * - Refreshes from CoinGecko API once per 24h.
 * - Falls back to bundled static data on failure.
 */
export function useSymbolMeta(): UseSymbolMetaResult {
  const [meta, setMeta] = useState<Record<string, SymbolMetaEntry>>({});
  const [isLoading, setIsLoading] = useState(true);
  const loadedRef = useRef(false);

  useEffect(() => {
    if (loadedRef.current) return;
    loadedRef.current = true;

    getSymbolMetadata()
      .then(setMeta)
      .catch(() => {
        // Already falls back internally, but just in case
      })
      .finally(() => setIsLoading(false));
  }, []);

  const getMeta = useCallback(
    (symbol: string): SymbolMetaEntry | null => {
      return lookupSymbol(meta, symbol);
    },
    [meta],
  );

  return { getMeta, isLoading };
}
