import React, { useState, useRef, useEffect } from "react";
import { Calendar, X, Clock } from "lucide-react";
import { useI18n } from "../i18n";
import type { HistoricalRange } from "../types";

interface DateRangePickerProps {
  onApply: (range: HistoricalRange) => void;
  onClear: () => void;
  active?: boolean;
}

const DateRangePicker: React.FC<DateRangePickerProps> = ({
  onApply,
  onClear,
  active = false,
}) => {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // Default to last 30 days
  const today = new Date();
  const thirtyDaysAgo = new Date(today);
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

  const toLocalISO = (d: Date): string => {
    const pad = (n: number) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  };

  const [startDt, setStartDt] = useState(toLocalISO(thirtyDaysAgo));
  const [endDt, setEndDt] = useState(toLocalISO(today));

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node))
        setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const handleApply = () => {
    const startMs = new Date(startDt).getTime();
    const endMs = new Date(endDt).getTime();
    if (isNaN(startMs) || isNaN(endMs) || startMs >= endMs) return;
    onApply({ startMs, endMs });
    setOpen(false);
  };

  const handleClear = () => {
    onClear();
    setOpen(false);
  };

  // Preset ranges
  const applyPreset = (hours: number) => {
    const end = new Date();
    const start = new Date(end.getTime() - hours * 3600_000);
    setStartDt(toLocalISO(start));
    setEndDt(toLocalISO(end));
  };

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen((v) => !v)}
        className={`flex items-center gap-1 px-2 py-1 rounded text-xs font-medium border transition-colors
          ${active ? "bg-amber-600 border-amber-500 text-white" : "border-gray-600 text-gray-400 hover:text-white hover:border-gray-400"}`}
        title={
          active
            ? t("historicalModeTooltip")
            : t("queryHistorical")
        }
      >
        {active ? <Clock size={12} /> : <Calendar size={12} />}
        {active ? t("historical") : t("history")}
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-1 z-[110] bg-gray-800 border border-gray-600 rounded-lg shadow-xl p-3 w-72">
          <div className="text-xs text-gray-400 mb-2 font-medium">
            {t("selectDateRange")}
          </div>

          {/* Presets */}
          <div className="flex gap-1 mb-3 flex-wrap">
            {[
              { label: "6H", hours: 6 },
              { label: "12H", hours: 12 },
              { label: "24H", hours: 24 },
              { label: "7D", hours: 7 * 24 },
              { label: "30D", hours: 30 * 24 },
              { label: "90D", hours: 90 * 24 },
              { label: "1Y", hours: 365 * 24 },
            ].map(({ label, hours }) => (
              <button
                key={label}
                onClick={() => applyPreset(hours)}
                className="px-2 py-0.5 text-xs bg-gray-700 hover:bg-gray-600 text-gray-300 rounded transition-colors"
              >
                {label}
              </button>
            ))}
          </div>

          {/* Datetime inputs */}
          <div className="space-y-2 mb-3">
            <div>
              <label className="block text-xs text-gray-500 mb-0.5">
                {t("start")}
              </label>
              <input
                type="datetime-local"
                value={startDt}
                onChange={(e) => setStartDt(e.target.value)}
                className="w-full px-2 py-1 bg-gray-700 border border-gray-600 rounded text-sm text-gray-200 focus:outline-none focus:border-blue-500"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-0.5">
                {t("end")}
              </label>
              <input
                type="datetime-local"
                value={endDt}
                onChange={(e) => setEndDt(e.target.value)}
                className="w-full px-2 py-1 bg-gray-700 border border-gray-600 rounded text-sm text-gray-200 focus:outline-none focus:border-blue-500"
              />
            </div>
          </div>

          {/* Actions */}
          <div className="flex gap-2">
            <button
              onClick={handleApply}
              className="flex-1 px-3 py-1.5 bg-blue-600 hover:bg-blue-500 text-white text-xs font-medium rounded transition-colors"
            >
              {t("apply")}
            </button>
            {active && (
              <button
                onClick={handleClear}
                className="flex items-center gap-1 px-3 py-1.5 bg-gray-700 hover:bg-gray-600 text-gray-300 text-xs font-medium rounded transition-colors"
              >
                <X size={10} /> {t("live")}
              </button>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default DateRangePicker;
