import React, { createContext, useContext, useState, useEffect } from 'react';

const AuthContext = createContext(null);

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000';
const TOKEN_KEY = 'mcap_auth_token';

export function AuthProvider({ children }) {
    const [user, setUser] = useState(null);      // { first_name, last_name, email }
    const [authLoading, setAuthLoading] = useState(true); // true while verifying token on mount

    /* ------------------------------------------------------------------ */
    /* Verify stored token on every page load                               */
    /* ------------------------------------------------------------------ */
    useEffect(() => {
        const token = localStorage.getItem(TOKEN_KEY);
        if (!token) {
            setAuthLoading(false);
            return;
        }
        fetch(`${API_URL}/api/auth/verify`, {
            headers: { Authorization: `Bearer ${token}` }
        })
            .then(r => r.json())
            .then(data => {
                if (data.valid) {
                    setUser(data.user);
                } else {
                    localStorage.removeItem(TOKEN_KEY);
                }
            })
            .catch(() => localStorage.removeItem(TOKEN_KEY))
            .finally(() => setAuthLoading(false));
    }, []);

    /* ------------------------------------------------------------------ */
    /* Helpers used by SignIn / SignUp pages                                */
    /* ------------------------------------------------------------------ */
    const login = (token, userData) => {
        localStorage.setItem(TOKEN_KEY, token);
        setUser(userData);
    };

    const logout = () => {
        localStorage.removeItem(TOKEN_KEY);
        setUser(null);
    };

    return (
        <AuthContext.Provider value={{ user, authLoading, login, logout }}>
            {children}
        </AuthContext.Provider>
    );
}

export function useAuth() {
    return useContext(AuthContext);
}
