// ─────────────────────────────────────────────────────────────────────────────
// Centralized error handling utilities
// ─────────────────────────────────────────────────────────────────────────────

/** Base application error with categorization */
export class AppError extends Error {
  public readonly code: string;
  public readonly isRetryable: boolean;
  public readonly originalError?: unknown;

  constructor(
    message: string,
    code: string = "UNKNOWN",
    isRetryable: boolean = false,
    originalError?: unknown,
  ) {
    super(message);
    this.name = "AppError";
    this.code = code;
    this.isRetryable = isRetryable;
    this.originalError = originalError;
  }
}

/** Network/API fetch errors */
export class NetworkError extends AppError {
  constructor(message: string, status?: number, originalError?: unknown) {
    super(
      message,
      status ? `HTTP_${status}` : "NETWORK",
      true, // network errors are retryable
      originalError,
    );
    this.name = "NetworkError";
  }
}

/** WebSocket connection/message errors */
export class WebSocketError extends AppError {
  constructor(message: string, originalError?: unknown) {
    super(message, "WS_ERROR", true, originalError);
    this.name = "WebSocketError";
  }
}

/** Data validation/parsing errors */
export class ValidationError extends AppError {
  constructor(message: string, originalError?: unknown) {
    super(message, "VALIDATION", false, originalError);
    this.name = "ValidationError";
  }
}

/** Authentication errors */
export class AuthError extends AppError {
  constructor(message: string, originalError?: unknown) {
    super(message, "AUTH", false, originalError);
    this.name = "AuthError";
  }
}

/**
 * Normalize any thrown value into a typed AppError.
 * Use in catch blocks to get consistent error objects.
 */
export function categorizeError(error: unknown): AppError {
  if (error instanceof AppError) return error;

  if (error instanceof TypeError && error.message.includes("fetch")) {
    return new NetworkError("Network connection failed", undefined, error);
  }

  if (error instanceof Error) {
    const msg = error.message;

    // Detect HTTP errors from our fetch wrappers
    if (msg.startsWith("API error")) {
      const status = parseInt(msg.replace("API error ", ""), 10);
      if (status === 401 || status === 403) {
        return new AuthError(msg, error);
      }
      return new NetworkError(msg, status, error);
    }

    return new AppError(msg, "UNKNOWN", false, error);
  }

  // Non-Error thrown values
  return new AppError(String(error), "UNKNOWN", false, error);
}
