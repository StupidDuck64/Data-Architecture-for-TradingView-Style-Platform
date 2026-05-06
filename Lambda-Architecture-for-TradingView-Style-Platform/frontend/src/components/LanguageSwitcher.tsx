import React, { useState, useRef, useEffect } from "react";
import { Globe } from "lucide-react";
import { useI18n } from "../i18n";

interface LangOption {
  code: string;
  label: string;
  flag: string;
}

const LANGS: LangOption[] = [
  { code: "en", label: "English", flag: "🇬🇧" },
  { code: "vi", label: "Tiếng Việt", flag: "🇻🇳" },
];

const LanguageSwitcher: React.FC = () => {
  const { lang, switchLang } = useI18n();
  const [isOpen, setIsOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node))
        setIsOpen(false);
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const current = LANGS.find((l) => l.code === lang) || LANGS[0];

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-1.5 px-2 py-1.5 rounded text-sm text-gray-300 hover:text-white hover:bg-gray-700 transition-colors"
        title="Language"
      >
        <Globe size={16} />
        <span className="hidden sm:inline">{current.flag}</span>
      </button>

      {isOpen && (
        <div className="absolute right-0 top-full mt-1 bg-gray-800 border border-gray-700 rounded-lg shadow-xl z-[150] overflow-hidden min-w-[140px]">
          {LANGS.map((l) => (
            <button
              key={l.code}
              onClick={() => {
                switchLang(l.code);
                setIsOpen(false);
              }}
              className={`w-full flex items-center gap-2 px-3 py-2 text-sm transition-colors ${
                lang === l.code
                  ? "bg-blue-600 text-white"
                  : "text-gray-300 hover:bg-gray-700"
              }`}
            >
              <span>{l.flag}</span>
              <span>{l.label}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
};

export default LanguageSwitcher;
