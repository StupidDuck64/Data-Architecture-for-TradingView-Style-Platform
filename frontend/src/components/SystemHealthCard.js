import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Info } from "lucide-react";

const REFRESH_MS = 3000;

function formatUptime(sec) {
  if (sec == null || Number.isNaN(sec)) return "-";
  const s = Math.max(0, Math.floor(sec));
  const days = Math.floor(s / 86400);
  const hours = Math.floor((s % 86400) / 3600);
  const minutes = Math.floor((s % 3600) / 60);
  const seconds = s % 60;

  if (days > 0) return `${days}d ${hours}h ${minutes}m`;
  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

const SystemHealthCard = () => {
  const [showTooltip, setShowTooltip] = useState(false);
  const [loading, setLoading] = useState(false);
  const [healthData, setHealthData] = useState(null);
  const [error, setError] = useState(null);
  const [nowMs, setNowMs] = useState(Date.now());

  const fetchHealth = useCallback(async () => {
    setLoading(true);
    setError(null);
    const started = performance.now();
    try {
      const res = await fetch("/api/health", {
        headers: { "Content-Type": "application/json" },
      });
      if (!res.ok) throw new Error(`Health API ${res.status}`);
      const data = await res.json();
      const apiRttMs = Number((performance.now() - started).toFixed(2));
      setHealthData({ ...data, api_rtt_ms: apiRttMs });
    } catch (e) {
      setError(e.message || "Failed to fetch system health");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!showTooltip) return;

    fetchHealth();
    const refreshId = setInterval(fetchHealth, REFRESH_MS);
    const tickId = setInterval(() => setNowMs(Date.now()), 1000);

    return () => {
      clearInterval(refreshId);
      clearInterval(tickId);
    };
  }, [showTooltip, fetchHealth]);

  const liveUptime = useMemo(() => {
    if (!healthData?.checked_at || healthData?.uptime_sec == null) {
      return healthData?.uptime_sec ?? null;
    }
    const checkedAtMs = new Date(healthData.checked_at).getTime();
    if (Number.isNaN(checkedAtMs)) return healthData.uptime_sec;
    const extraSec = Math.max(0, Math.floor((nowMs - checkedAtMs) / 1000));
    return Number(healthData.uptime_sec) + extraSec;
  }, [healthData, nowMs]);

  return (
    <div
      className="relative"
      onMouseEnter={() => setShowTooltip(true)}
      onMouseLeave={() => setShowTooltip(false)}
    >
      <button
        type="button"
        aria-label="System diagnostics"
        className="text-gray-400 hover:text-blue-300 transition-colors"
      >
        <Info className="w-4 h-4" />
      </button>

      {showTooltip && (
        <div className="absolute top-full left-0 mt-2 w-72 p-3 rounded-lg border border-gray-600 bg-gray-900 shadow-2xl z-[220] text-xs">
          <div className="flex items-center justify-between mb-2">
            <span className="font-semibold text-gray-200">Status</span>
            {healthData?.status && (
              <span
                className={`px-1.5 py-0.5 rounded font-medium ${
                  healthData.status === "ok"
                    ? "bg-green-900/70 text-green-300"
                    : "bg-amber-900/70 text-amber-300"
                }`}
              >
                {healthData.status}
              </span>
            )}
          </div>

          {loading && !healthData && (
            <div className="text-gray-400">Loading health metrics...</div>
          )}

          {error && !healthData && <div className="text-red-400">{error}</div>}

          {healthData && (
            <div className="space-y-1.5">
              <div className="grid grid-cols-2 gap-1">
                <span className="text-gray-400">API RTT</span>
                <span className="text-gray-200 text-right">{healthData.api_rtt_ms ?? "-"} ms</span>

                <span className="text-gray-400">Server Health</span>
                <span className="text-gray-200 text-right">{healthData.status || "-"}</span>

                <span className="text-gray-400">Total Check</span>
                <span className="text-gray-200 text-right">{healthData.total_latency_ms ?? "-"} ms</span>

                <span className="text-gray-400">Uptime</span>
                <span className="text-gray-200 text-right">{formatUptime(liveUptime)}</span>
              </div>

              <div className="border-t border-gray-700 pt-1.5">
                <div className="text-gray-400 mb-1">Dependencies</div>
                {[{key: "keydb", label: "Cache"}, {key: "influxdb", label: "Time-Series DB"}, {key: "trino", label: "Query Engine"}].map(({key: svc, label}) => {
                  const status = healthData.checks?.[svc];
                  const latency = healthData.latency_ms?.[`${svc}_ms`];
                  const ok = status === "ok";
                  return (
                    <div key={svc} className="flex items-center justify-between">
                      <span className={ok ? "text-gray-300" : "text-red-300"}>{label}</span>
                      <span className={ok ? "text-gray-300" : "text-red-300"}>
                        {ok ? `${latency ?? "-"} ms` : "error"}
                      </span>
                    </div>
                  );
                })}
              </div>

              <div className="text-[11px] text-gray-500 pt-1 border-t border-gray-700">
                Updated: {healthData.checked_at ? new Date(healthData.checked_at).toLocaleString() : "-"}
              </div>

              {error && <div className="text-[11px] text-amber-400">Warning: {error}</div>}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default SystemHealthCard;
