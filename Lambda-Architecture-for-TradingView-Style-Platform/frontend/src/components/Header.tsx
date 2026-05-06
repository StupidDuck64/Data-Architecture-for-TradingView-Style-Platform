import React, { useEffect, useRef } from "react";
import { Search, Menu, X } from "lucide-react";
import LanguageSwitcher from "./LanguageSwitcher";
import SystemHealthCard from "./SystemHealthCard";
import { useI18n } from "../i18n";
import type { TranslationKey } from "../i18n/translations";

const NAV_ITEMS_KEYS: TranslationKey[] = [
  "products",
  "community",
  "markets",
  "news",
  "brokers",
];

interface HeaderProps {
  showNavDrawer: boolean;
  onToggleDrawer: (open: boolean) => void;
}

const Header: React.FC<HeaderProps> = ({ showNavDrawer, onToggleDrawer }) => {
  const { t } = useI18n();
  const drawerRef = useRef<HTMLDivElement>(null);

  // Close drawer on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (drawerRef.current && !drawerRef.current.contains(e.target as Node)) {
        onToggleDrawer(false);
      }
    };
    if (showNavDrawer) document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [showNavDrawer, onToggleDrawer]);

  return (
    <>
      <header className="bg-gray-800 p-4 flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <div className="flex items-center gap-2">
            <span className="text-2xl font-bold text-blue-500">LMView</span>
            <SystemHealthCard />
          </div>
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
          <button
            onClick={() => onToggleDrawer(true)}
            className="bg-blue-600 rounded-full p-2 hover:bg-blue-700 transition-colors duration-200"
          >
            <Menu className="w-5 h-5" />
          </button>
        </div>
      </header>

      {showNavDrawer && (
        <div className="fixed inset-0 bg-black bg-opacity-50 z-[300]">
          <div
            ref={drawerRef}
            className="absolute right-0 top-0 h-full w-64 bg-gray-800 shadow-2xl flex flex-col"
          >
            <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700">
              <span className="text-lg font-bold text-blue-500">LMView</span>
              <button
                onClick={() => onToggleDrawer(false)}
                className="text-gray-400 hover:text-white transition-colors"
              >
                <X size={20} />
              </button>
            </div>
            <nav className="flex flex-col p-4 space-y-1">
              {NAV_ITEMS_KEYS.map((key) => (
                <a
                  key={key}
                  href="#"
                  onClick={() => onToggleDrawer(false)}
                  className="px-4 py-2.5 rounded-lg text-gray-300 hover:text-white hover:bg-gray-700 transition-colors duration-150 font-medium"
                >
                  {t(key)}
                </a>
              ))}
            </nav>
          </div>
        </div>
      )}
    </>
  );
};

export default Header;
