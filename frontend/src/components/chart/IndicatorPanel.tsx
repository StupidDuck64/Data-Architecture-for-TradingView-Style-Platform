import React, { useState } from "react";
import { Settings, ChevronDown, ChevronUp } from "lucide-react";
import { useI18n } from "../../i18n";
import type { IndicatorSettings } from "../../types";

interface IndicatorDef {
  key: string;
  label: string;
}

const INDICATORS: IndicatorDef[] = [
  { key: "volume", label: "Volume" },
  { key: "sma20", label: "SMA 20" },
  { key: "sma50", label: "SMA 50" },
  { key: "ema", label: "EMA" },
  { key: "rsi", label: "RSI" },
  { key: "mfi", label: "MFI" },
];

interface IndicatorPanelProps {
  indSettings: Record<string, IndicatorSettings>;
  onChange: (settings: Record<string, IndicatorSettings>) => void;
}

const IndicatorPanel: React.FC<IndicatorPanelProps> = ({ indSettings, onChange }) => {
  const { t } = useI18n();
  const [expanded, setExpanded] = useState<string | null>(null);

  const set = (key: string, field: string, value: unknown) => {
    onChange({
      ...indSettings,
      [key]: { ...indSettings[key], [field]: value },
    });
  };

  const toggleVisible = (key: string) => set(key, "visible", !indSettings[key].visible);

  return (
    <div className="mt-1 w-56 bg-gray-800 border border-gray-700 rounded-lg shadow-2xl overflow-hidden">
      <div className="px-3 py-2 bg-gray-750 border-b border-gray-700 text-xs font-semibold text-gray-300 flex items-center gap-1">
        <Settings size={11} /> {t("technicalIndicators")}
      </div>
      {INDICATORS.map(({ key, label }) => {
        const cfg = indSettings[key] || {};
        const isOpen = expanded === key;
        return (
          <div key={key} className="border-b border-gray-700 last:border-0">
            <div
              className="flex items-center justify-between px-3 py-1.5 hover:bg-gray-700 cursor-pointer"
              onClick={() => setExpanded(isOpen ? null : key)}
            >
              <div className="flex items-center gap-2">
                {cfg.color && (
                  <span
                    className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                    style={{ backgroundColor: cfg.color }}
                  />
                )}
                <span className="text-xs text-gray-300">{label}</span>
              </div>
              <div className="flex items-center gap-1.5">
                <button
                  onClick={(e) => { e.stopPropagation(); toggleVisible(key); }}
                  className={`w-8 h-4 rounded-full transition-colors ${cfg.visible ? "bg-blue-600" : "bg-gray-600"}`}
                >
                  <span className={`block w-3 h-3 rounded-full bg-white shadow mx-0.5 transition-transform ${cfg.visible ? "translate-x-4" : "translate-x-0"}`} />
                </button>
                {isOpen ? <ChevronUp size={12} className="text-gray-400" /> : <ChevronDown size={12} className="text-gray-400" />}
              </div>
            </div>
            {isOpen && (
              <div className="px-3 pb-2 space-y-1.5 bg-gray-900">
                {cfg.period !== undefined && (
                  <div className="flex items-center justify-between gap-2 mt-1.5">
                    <span className="text-xs text-gray-400">{t("period")}</span>
                    <input type="number" min="1" max="500" value={cfg.period}
                      onChange={(e) => set(key, "period", parseInt(e.target.value) || cfg.period)}
                      className="w-16 bg-gray-700 text-white text-xs rounded px-2 py-0.5 border border-gray-600 focus:outline-none" />
                  </div>
                )}
                {cfg.color !== undefined && (
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-xs text-gray-400">{t("color")}</span>
                    <input type="color" value={cfg.color} onChange={(e) => set(key, "color", e.target.value)}
                      className="w-8 h-5 rounded cursor-pointer border-0 bg-transparent" />
                  </div>
                )}
                {cfg.lineWidth !== undefined && (
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-xs text-gray-400">{t("thickness")}</span>
                    <input type="range" min="0.5" max="4" step="0.5" value={cfg.lineWidth}
                      onChange={(e) => set(key, "lineWidth", parseFloat(e.target.value))}
                      className="w-20 accent-blue-500" />
                    <span className="text-xs text-gray-300 w-4">{cfg.lineWidth}</span>
                  </div>
                )}
                {cfg.overbought !== undefined && (
                  <>
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-xs text-gray-400">{t("overbought")}</span>
                      <input type="number" min="50" max="100" value={cfg.overbought}
                        onChange={(e) => set(key, "overbought", parseInt(e.target.value))}
                        className="w-16 bg-gray-700 text-white text-xs rounded px-2 py-0.5 border border-gray-600 focus:outline-none" />
                    </div>
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-xs text-gray-400">{t("oversold")}</span>
                      <input type="number" min="0" max="50" value={cfg.oversold}
                        onChange={(e) => set(key, "oversold", parseInt(e.target.value))}
                        className="w-16 bg-gray-700 text-white text-xs rounded px-2 py-0.5 border border-gray-600 focus:outline-none" />
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};

export default IndicatorPanel;
