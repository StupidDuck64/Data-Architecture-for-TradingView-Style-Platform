import React, { createContext, useContext, useState, useCallback } from "react";
import translations, { type TranslationKey } from "./translations";

interface I18nContextValue {
  lang: string;
  switchLang: (lang: string) => void;
  t: (key: TranslationKey) => string;
}

const I18nContext = createContext<I18nContextValue | null>(null);

export function I18nProvider({ children }: { children: React.ReactNode }) {
  const [lang, setLang] = useState<string>(() => {
    try {
      return localStorage.getItem("app_lang") || "en";
    } catch {
      return "en";
    }
  });

  const switchLang = useCallback((newLang: string) => {
    setLang(newLang);
    try {
      localStorage.setItem("app_lang", newLang);
    } catch {
      // Storage unavailable
    }
  }, []);

  const t = useCallback(
    (key: TranslationKey): string => {
      const langTranslations =
        translations[lang as keyof typeof translations];
      return (
        (langTranslations && langTranslations[key]) ||
        translations["en"][key] ||
        key
      );
    },
    [lang],
  );

  return (
    <I18nContext.Provider value={{ lang, switchLang, t }}>
      {children}
    </I18nContext.Provider>
  );
}

export function useI18n(): I18nContextValue {
  const ctx = useContext(I18nContext);
  if (!ctx) throw new Error("useI18n must be used within I18nProvider");
  return ctx;
}
