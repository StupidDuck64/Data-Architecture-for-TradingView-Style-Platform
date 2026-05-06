import React, { createContext, useContext, useState, useCallback } from "react";
import type { UserSession, AuthResult } from "../types";

interface AuthContextValue {
  user: UserSession | null;
  login: (email: string, password: string) => AuthResult;
  register: (name: string, email: string, password: string) => AuthResult;
  logout: () => void;
}

interface StoredUser {
  name: string;
  email: string;
  password: string;
}

const AuthContext = createContext<AuthContextValue | null>(null);

// Mock user storage using localStorage
function getStoredUsers(): StoredUser[] {
  try {
    return JSON.parse(localStorage.getItem("app_users") || "[]") as StoredUser[];
  } catch {
    return [];
  }
}

function storeUsers(users: StoredUser[]): void {
  try {
    localStorage.setItem("app_users", JSON.stringify(users));
  } catch {
    // Storage unavailable
  }
}

function getStoredSession(): UserSession | null {
  try {
    const s = localStorage.getItem("app_session");
    return s ? (JSON.parse(s) as UserSession) : null;
  } catch {
    return null;
  }
}

function storeSession(session: UserSession | null): void {
  try {
    if (session) localStorage.setItem("app_session", JSON.stringify(session));
    else localStorage.removeItem("app_session");
  } catch {
    // Storage unavailable
  }
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<UserSession | null>(() => getStoredSession());

  const login = useCallback((email: string, password: string): AuthResult => {
    const users = getStoredUsers();
    const found = users.find(
      (u) => u.email === email && u.password === password,
    );
    if (!found) return { success: false, error: "invalidCredentials" };
    const session: UserSession = { email: found.email, name: found.name };
    setUser(session);
    storeSession(session);
    return { success: true };
  }, []);

  const register = useCallback((name: string, email: string, password: string): AuthResult => {
    const users = getStoredUsers();
    if (users.find((u) => u.email === email)) {
      return { success: false, error: "emailExists" };
    }
    users.push({ name, email, password });
    storeUsers(users);
    const session: UserSession = { email, name };
    setUser(session);
    storeSession(session);
    return { success: true };
  }, []);

  const logout = useCallback(() => {
    setUser(null);
    storeSession(null);
  }, []);

  return (
    <AuthContext.Provider value={{ user, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
