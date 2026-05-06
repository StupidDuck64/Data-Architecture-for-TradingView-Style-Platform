import React, { useState, useRef, useCallback } from "react";
import { Settings } from "lucide-react";
import ToolSettingsPopup, { DEFAULT_TOOL_SETTINGS, type ToolSettings } from "./ToolSettingsPopup";
import { useI18n } from "../i18n";
import type { TranslationKey } from "../i18n/translations";

const SETTINGS_TOOLS = new Set(["trendline", "horizontal", "rectangle", "fibRetracement", "ruler", "elliottWave", "harmonicABCD"]);

interface ToolDef {
  id: string;
  labelKey: TranslationKey;
  icon: React.ReactNode;
}

interface ToolGroup {
  labelKey: TranslationKey;
  tools: ToolDef[];
}

const TOOL_GROUPS: ToolGroup[] = [
  {
    labelKey: "basic",
    tools: [
      { id: "cursor", labelKey: "cursor", icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><path d="M4 4l7 17 2.5-6.5L20 12z" /></svg> },
      { id: "ruler", labelKey: "ruler", icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><path d="M2 22L22 2" /><path d="M6 18l2-2" /><path d="M10 14l2-2" /><path d="M14 10l2-2" /><path d="M18 6l2-2" /></svg> },
      { id: "trendline", labelKey: "trendline", icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><path d="M3 20L21 4" /><circle cx="3" cy="20" r="2" fill="currentColor" /><circle cx="21" cy="4" r="2" fill="currentColor" /></svg> },
      { id: "horizontal", labelKey: "horizontalLine", icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><path d="M2 12h20" strokeDasharray="4 2" /></svg> },
      { id: "rectangle", labelKey: "rectangle", icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><rect x="3" y="5" width="18" height="14" rx="1" /></svg> },
      { id: "fibRetracement", labelKey: "fibonacci", icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><path d="M2 4h20" /><path d="M2 9h20" opacity="0.7" /><path d="M2 14h20" opacity="0.5" /><path d="M2 20h20" opacity="0.3" /></svg> },
      { id: "text", labelKey: "textNotes", icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><path d="M4 7V4h16v3" /><path d="M12 4v16" /><path d="M8 20h8" /></svg> },
    ],
  },
  {
    labelKey: "patterns",
    tools: [
      { id: "elliottWave", labelKey: "elliottWave", icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><path d="M2 16 L5 8 L8 14 L11 6 L14 12 L17 5 L20 10" strokeLinejoin="round" /><circle cx="5" cy="8" r="1.5" fill="currentColor" /><circle cx="11" cy="6" r="1.5" fill="currentColor" /><circle cx="17" cy="5" r="1.5" fill="currentColor" /></svg> },
      { id: "harmonicABCD", labelKey: "harmonicABCD", icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><path d="M3 18 L8 6 L13 14 L20 4" strokeLinejoin="round" /><circle cx="3" cy="18" r="1.5" fill="currentColor" /><circle cx="8" cy="6" r="1.5" fill="currentColor" /><circle cx="13" cy="14" r="1.5" fill="currentColor" /><circle cx="20" cy="4" r="1.5" fill="currentColor" /></svg> },
    ],
  },
  {
    labelKey: "delete",
    tools: [
      { id: "eraser", labelKey: "clearAll", icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5"><path d="M3 6h18" /><path d="M8 6V4h8v2" /><path d="M5 6l1 14h12l1-14" /><path d="M10 10v8" /><path d="M14 10v8" /></svg> },
    ],
  },
];

interface DrawingToolbarProps {
  activeTool: string;
  onToolChange: (toolId: string) => void;
  onClearAll: () => void;
  toolSettings?: Record<string, ToolSettings>;
  onToolSettingsChange?: (toolId: string, settings: ToolSettings) => void;
}

const DrawingToolbar: React.FC<DrawingToolbarProps> = ({ activeTool, onToolChange, onClearAll, toolSettings, onToolSettingsChange }) => {
  const { t } = useI18n();
  const [openSettings, setOpenSettings] = useState<string | null>(null);
  const btnRefs = useRef<Record<string, HTMLDivElement | null>>({});

  const handleToolClick = useCallback((toolId: string) => {
    if (toolId === "eraser") { onClearAll(); return; }
    onToolChange(toolId);
  }, [onToolChange, onClearAll]);

  const handleSettingsClick = useCallback((e: React.MouseEvent, toolId: string) => {
    e.stopPropagation();
    setOpenSettings((prev) => (prev === toolId ? null : toolId));
  }, []);

  return (
    <div className="flex flex-col items-center bg-gray-800 rounded-lg py-2 px-1 space-y-1 shadow-lg border border-gray-700 select-none">
      {TOOL_GROUPS.map((group, gi) => (
        <React.Fragment key={group.labelKey}>
          {gi > 0 && <div className="w-6 border-t border-gray-700 my-0.5" />}
          {group.tools.map((tool) => {
            const isActive = activeTool === tool.id;
            const hasSettings = SETTINGS_TOOLS.has(tool.id);
            return (
              <div key={tool.id} ref={(el) => { btnRefs.current[tool.id] = el; }} className="relative group">
                <button title={t(tool.labelKey)} onClick={() => handleToolClick(tool.id)}
                  className={`p-2 rounded-md transition-all duration-150 ${isActive ? "bg-blue-600 text-white shadow-md" : "text-gray-400 hover:text-white hover:bg-gray-700"}`}>
                  {tool.icon}
                </button>
                {hasSettings && (
                  <button onClick={(e) => handleSettingsClick(e, tool.id)}
                    className={`absolute -right-1 -bottom-1 w-4 h-4 rounded-full flex items-center justify-center transition-all ${openSettings === tool.id ? "bg-blue-400 text-white opacity-100" : "bg-gray-600 text-gray-300 opacity-0 group-hover:opacity-100"}`}
                    title={t("settings")}>
                    <Settings size={9} />
                  </button>
                )}
                <span className="absolute left-full ml-2 px-2 py-1 bg-gray-900 text-white text-xs rounded whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity duration-150 z-50 border border-gray-600">
                  {t(tool.labelKey)}
                </span>
                {openSettings === tool.id && hasSettings && (
                  <ToolSettingsPopup tool={tool.id} settings={toolSettings?.[tool.id] || DEFAULT_TOOL_SETTINGS[tool.id]}
                    onChange={(ns) => onToolSettingsChange?.(tool.id, ns)} onClose={() => setOpenSettings(null)}
                    anchorRef={{ current: btnRefs.current[tool.id] ?? null }} />
                )}
              </div>
            );
          })}
        </React.Fragment>
      ))}
    </div>
  );
};

export default DrawingToolbar;
