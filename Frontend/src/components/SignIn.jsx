import React, { useState } from 'react';
import { useAuth } from '../context/AuthContext';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000';

export default function SignIn({ onSwitchToSignUp, successMessage }) {
    const { login } = useAuth();
    const [formData, setFormData] = useState({ email: '', password: '' });
    const [error, setError]     = useState('');
    const [loading, setLoading] = useState(false);

    const handleChange = e => {
        setFormData(prev => ({ ...prev, [e.target.name]: e.target.value }));
        setError('');
    };

    const handleSubmit = async e => {
        e.preventDefault();
        setLoading(true);
        setError('');

        try {
            const res = await fetch(`${API_URL}/api/auth/signin`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(formData)
            });
            const data = await res.json();
            if (!res.ok) { setError(data.error || 'Sign in failed'); return; }
            login(data.token, data.user);
        } catch {
            setError('Network error. Please try again.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="auth-page">
            <div className="auth-bg-pattern" />

            <div className="auth-card">
                {/* Branded top strip */}
                <div className="auth-card-header">
                    <div className="auth-logo">
                        <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                            <line x1="12" y1="20" x2="12" y2="10" />
                            <line x1="18" y1="20" x2="18" y2="4" />
                            <line x1="6"  y1="20" x2="6"  y2="16" />
                        </svg>
                    </div>
                    <span className="auth-card-title">Market Cap Tool</span>
                </div>

                <div className="auth-card-body">
                    <h2 className="auth-heading">Welcome back</h2>
                    <p className="auth-sub">Sign in to access your dashboard</p>

                    {successMessage && (
                        <div className="auth-success" role="status">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
                                <polyline points="22 4 12 14.01 9 11.01" />
                            </svg>
                            {successMessage}
                        </div>
                    )}

                    {error && (
                        <div className="auth-error" role="alert">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                <circle cx="12" cy="12" r="10" />
                                <line x1="12" y1="8" x2="12" y2="12" />
                                <line x1="12" y1="16" x2="12.01" y2="16" />
                            </svg>
                            {error}
                        </div>
                    )}

                    <form onSubmit={handleSubmit} className="auth-form" noValidate>
                        <div className="auth-field">
                            <label htmlFor="signin-email" className="auth-label">Email address</label>
                            <input
                                id="signin-email"
                                type="email"
                                name="email"
                                className="auth-input"
                                placeholder="you@example.com"
                                value={formData.email}
                                onChange={handleChange}
                                autoFocus
                                required
                            />
                        </div>

                        <div className="auth-field">
                            <label htmlFor="signin-password" className="auth-label">Password</label>
                            <input
                                id="signin-password"
                                type="password"
                                name="password"
                                className="auth-input"
                                placeholder="••••••••"
                                value={formData.password}
                                onChange={handleChange}
                                required
                            />
                        </div>

                        <button
                            id="signin-submit-btn"
                            type="submit"
                            className={`auth-btn ${loading ? 'auth-btn--loading' : ''}`}
                            disabled={loading}
                        >
                            {loading ? (
                                <>
                                    <span className="auth-spinner" />
                                    Signing in…
                                </>
                            ) : 'Sign In'}
                        </button>
                    </form>

                    <p className="auth-switch-text">
                        Don't have an account?{' '}
                        <button className="auth-link-btn" onClick={onSwitchToSignUp} id="goto-signup-btn">
                            Create one
                        </button>
                    </p>
                </div>
            </div>
        </div>
    );
}
