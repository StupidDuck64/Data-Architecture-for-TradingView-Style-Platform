import { useState, useCallback, useRef, useEffect } from "react";
import { categorizeError, type AppError } from "../utils/errors";

interface UseApiCallOptions {
  /** Number of automatic retry attempts for retryable errors (default: 2) */
  retryCount?: number;
  /** Delay between retries in ms (default: 1000) */
  retryDelayMs?: number;
  /** Polling interval in ms — if set, re-fetches on this interval */
  pollIntervalMs?: number;
  /** Callback on error */
  onError?: (error: AppError) => void;
}

interface UseApiCallResult<T> {
  data: T | null;
  error: AppError | null;
  isLoading: boolean;
  retry: () => void;
}

/**
 * Generic hook for API calls with automatic retry, error categorization,
 * and optional polling.
 *
 * Replaces the duplicated useState(error) + useEffect + fetch + .catch pattern
 * found across OrderBook, RecentTrades, SystemHealthCard, etc.
 *
 * @example
 * const { data, error, isLoading, retry } = useApiCall(
 *   () => fetchOrderBook(symbol),
 *   { pollIntervalMs: 2000 }
 * );
 */
export function useApiCall<T>(
  fetcher: () => Promise<T>,
  options: UseApiCallOptions = {},
): UseApiCallResult<T> {
  const {
    retryCount = 2,
    retryDelayMs = 1000,
    pollIntervalMs,
    onError,
  } = options;

  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<AppError | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  const cancelledRef = useRef(false);
  const retriesLeftRef = useRef(retryCount);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;

  const execute = useCallback(async () => {
    if (cancelledRef.current) return;
    setIsLoading(true);

    try {
      const result = await fetcherRef.current();
      if (cancelledRef.current) return;
      setData(result);
      setError(null);
      retriesLeftRef.current = retryCount;
    } catch (err) {
      if (cancelledRef.current) return;
      const appError = categorizeError(err);

      if (appError.isRetryable && retriesLeftRef.current > 0) {
        retriesLeftRef.current -= 1;
        setTimeout(() => {
          if (!cancelledRef.current) execute();
        }, retryDelayMs);
        return;
      }

      setError(appError);
      onErrorRef.current?.(appError);
    } finally {
      if (!cancelledRef.current) setIsLoading(false);
    }
  }, [retryCount, retryDelayMs]);

  // Initial fetch
  useEffect(() => {
    cancelledRef.current = false;
    retriesLeftRef.current = retryCount;
    execute();

    return () => {
      cancelledRef.current = true;
    };
  }, [execute, retryCount]);

  // Optional polling
  useEffect(() => {
    if (!pollIntervalMs) return;
    const id = setInterval(execute, pollIntervalMs);
    return () => clearInterval(id);
  }, [execute, pollIntervalMs]);

  const retry = useCallback(() => {
    retriesLeftRef.current = retryCount;
    setError(null);
    execute();
  }, [execute, retryCount]);

  return { data, error, isLoading, retry };
}
