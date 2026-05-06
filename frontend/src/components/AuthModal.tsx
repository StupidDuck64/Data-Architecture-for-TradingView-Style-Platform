import React, { useState } from "react";
import { X } from "lucide-react";
import { useAuth } from "../contexts/AuthContext";
import { useI18n } from "../i18n";

interface AuthModalProps {
  isOpen: boolean;
  onClose: () => void;
}

const AuthModal: React.FC<AuthModalProps> = ({ isOpen, onClose }) => {
  const { login, register } = useAuth();
  const { t } = useI18n();
  const [mode, setMode] = useState<"login" | "register">("login");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPw, setConfirmPw] = useState("");
  const [error, setError] = useState("");

  if (!isOpen) return null;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    if (mode === "register") {
      if (password !== confirmPw) {
        setError(t("passwordsMismatch"));
        return;
      }
      if (password.length < 6) {
        setError(t("passwordMinLength"));
        return;
      }
      const result = register(name, email, password);
      if (!result.success) {
        setError(result.error ? t(result.error as Parameters<typeof t>[0]) : "");
        return;
      }
    } else {
      const result = login(email, password);
      if (!result.success) {
        setError(result.error ? t(result.error as Parameters<typeof t>[0]) : "");
        return;
      }
    }
    onClose();
  };

  const switchMode = () => {
    setMode(mode === "login" ? "register" : "login");
    setError("");
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-60 flex items-center justify-center z-[200]">
      <div className="bg-gray-800 rounded-xl shadow-2xl w-96 max-w-[90vw] p-6 relative">
        <button
          onClick={onClose}
          className="absolute top-3 right-3 text-gray-400 hover:text-white"
        >
          <X size={20} />
        </button>

        <h2 className="text-xl font-bold text-white mb-6">
          {mode === "login" ? t("loginTitle") : t("registerTitle")}
        </h2>

        <form onSubmit={handleSubmit} className="space-y-4">
          {mode === "register" && (
            <div>
              <label className="block text-sm text-gray-400 mb-1">
                {t("name")}
              </label>
              <input
                type="text"
                required
                value={name}
                onChange={(e) => setName(e.target.value)}
                className="w-full bg-gray-700 text-white rounded-lg px-3 py-2 border border-gray-600 focus:outline-none focus:border-blue-500"
                placeholder={t("name")}
              />
            </div>
          )}
          <div>
            <label className="block text-sm text-gray-400 mb-1">
              {t("email")}
            </label>
            <input
              type="email"
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full bg-gray-700 text-white rounded-lg px-3 py-2 border border-gray-600 focus:outline-none focus:border-blue-500"
              placeholder="email@example.com"
            />
          </div>
          <div>
            <label className="block text-sm text-gray-400 mb-1">
              {t("password")}
            </label>
            <input
              type="password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full bg-gray-700 text-white rounded-lg px-3 py-2 border border-gray-600 focus:outline-none focus:border-blue-500"
              placeholder="••••••"
            />
          </div>
          {mode === "register" && (
            <div>
              <label className="block text-sm text-gray-400 mb-1">
                {t("confirmPassword")}
              </label>
              <input
                type="password"
                required
                value={confirmPw}
                onChange={(e) => setConfirmPw(e.target.value)}
                className="w-full bg-gray-700 text-white rounded-lg px-3 py-2 border border-gray-600 focus:outline-none focus:border-blue-500"
                placeholder="••••••"
              />
            </div>
          )}

          {error && <p className="text-red-400 text-sm">{error}</p>}

          <button
            type="submit"
            className="w-full bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg py-2 transition-colors"
          >
            {mode === "login" ? t("signIn") : t("signUp")}
          </button>
        </form>

        <p className="text-center text-sm text-gray-400 mt-4">
          {mode === "login" ? t("noAccount") : t("hasAccount")}{" "}
          <button
            onClick={switchMode}
            className="text-blue-400 hover:text-blue-300"
          >
            {mode === "login" ? t("signUp") : t("signIn")}
          </button>
        </p>
      </div>
    </div>
  );
};

export default AuthModal;
