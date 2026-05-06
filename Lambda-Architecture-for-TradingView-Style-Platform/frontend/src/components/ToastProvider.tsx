import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { AlertTriangle, Info, X, XCircle } from "lucide-react";

// ─── Types ────────────────────────────────────────────────────────
type ToastType = "error" | "warning" | "info";

interface Toast {
  id: string;
  type: ToastType;
  message: string;
  dismissMs: number;
}

interface ToastContextValue {
  showError: (message: string) => void;
  showWarning: (message: string) => void;
  showInfo: (message: string) => void;
}

// ─── Context ──────────────────────────────────────────────────────
const ToastContext = createContext<ToastContextValue | null>(null);

let _toastId = 0;

// ─── Provider ─────────────────────────────────────────────────────
export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const timersRef = useRef<Map<string, ReturnType<typeof setTimeout>>>(
    new Map(),
  );

  const dismiss = useCallback((id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
    const timer = timersRef.current.get(id);
    if (timer) {
      clearTimeout(timer);
      timersRef.current.delete(id);
    }
  }, []);

  const show = useCallback(
    (type: ToastType, message: string, dismissMs: number = 5000) => {
      const id = `toast-${++_toastId}`;
      const toast: Toast = { id, type, message, dismissMs };

      setToasts((prev) => [...prev.slice(-4), toast]); // keep max 5

      const timer = setTimeout(() => dismiss(id), dismissMs);
      timersRef.current.set(id, timer);
    },
    [dismiss],
  );

  const showError = useCallback(
    (message: string) => show("error", message, 8000),
    [show],
  );
  const showWarning = useCallback(
    (message: string) => show("warning", message, 6000),
    [show],
  );
  const showInfo = useCallback(
    (message: string) => show("info", message, 4000),
    [show],
  );

  // Cleanup all timers on unmount
  useEffect(() => {
    return () => {
      timersRef.current.forEach((timer) => clearTimeout(timer));
    };
  }, []);

  return (
    <ToastContext.Provider value={{ showError, showWarning, showInfo }}>
      {children}

      {/* Toast stack — fixed bottom-right */}
      {toasts.length > 0 && (
        <div className="fixed bottom-4 right-4 z-[9999] flex flex-col gap-2 max-w-sm">
          {toasts.map((toast) => (
            <ToastItem key={toast.id} toast={toast} onDismiss={dismiss} />
          ))}
        </div>
      )}
    </ToastContext.Provider>
  );
}

// ─── Toast item ───────────────────────────────────────────────────
const STYLES: Record<ToastType, { border: string; icon: string; bg: string }> =
  {
    error: {
      border: "border-l-red-500",
      icon: "text-red-400",
      bg: "bg-red-950/30",
    },
    warning: {
      border: "border-l-amber-500",
      icon: "text-amber-400",
      bg: "bg-amber-950/30",
    },
    info: {
      border: "border-l-blue-500",
      icon: "text-blue-400",
      bg: "bg-blue-950/30",
    },
  };

const ICONS: Record<ToastType, React.ReactNode> = {
  error: <XCircle size={18} />,
  warning: <AlertTriangle size={18} />,
  info: <Info size={18} />,
};

function ToastItem({
  toast,
  onDismiss,
}: {
  toast: Toast;
  onDismiss: (id: string) => void;
}) {
  const style = STYLES[toast.type];

  return (
    <div
      className={`flex items-start gap-2.5 px-4 py-3 rounded-lg border-l-4 ${style.border} ${style.bg} bg-gray-900/95 backdrop-blur-sm shadow-2xl text-sm text-gray-200 animate-slide-in-right`}
      role="alert"
    >
      <span className={`mt-0.5 flex-shrink-0 ${style.icon}`}>
        {ICONS[toast.type]}
      </span>
      <span className="flex-1 leading-snug">{toast.message}</span>
      <button
        onClick={() => onDismiss(toast.id)}
        className="flex-shrink-0 text-gray-500 hover:text-gray-300 transition-colors"
      >
        <X size={14} />
      </button>
    </div>
  );
}

// ─── Hook ─────────────────────────────────────────────────────────
export function useToast(): ToastContextValue {
  const ctx = useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used within ToastProvider");
  return ctx;
}
