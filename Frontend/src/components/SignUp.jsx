import React, { useState } from 'react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000';

export default function SignUp({ onSwitchToSignIn, onSignupSuccess }) {
    const [formData, setFormData] = useState({
        first_name: '', last_name: '', email: '', password: '', confirm_password: ''
    });
    const [error, setError]     = useState('');
    const [loading, setLoading] = useState(false);

    const handleChange = e => {
        setFormData(prev => ({ ...prev, [e.target.name]: e.target.value }));
        setError('');
    };

    const handleSubmit = async e => {
        e.preventDefault();
        if (formData.password !== formData.confirm_password) {
            setError('Passwords do not match');
            return;
        }
        setLoading(true);
        setError('');

        try {
            const res = await fetch(`${API_URL}/api/auth/signup`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(formData)
            });
            const data = await res.json();
            if (!res.ok) { setError(data.error || 'Sign up failed'); return; }
            // Don't auto-login — redirect to Sign In with a success message
            if (onSignupSuccess) onSignupSuccess(data.user.first_name);
            onSwitchToSignIn();
        } catch {
            setError('Network error. Please try again.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="auth-page">
            <div className="auth-bg-pattern" />

            <div className="auth-card auth-card--wide">
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
                    <h2 className="auth-heading">Create your account</h2>
                    <p className="auth-sub">Start consolidating market cap data in minutes</p>

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
                        {/* Name Row */}
                        <div className="auth-row">
                            <div className="auth-field">
                                <label htmlFor="signup-first" className="auth-label">First name</label>
                                <input
                                    id="signup-first"
                                    type="text"
                                    name="first_name"
                                    className="auth-input"
                                    placeholder="Prachi"
                                    value={formData.first_name}
                                    onChange={handleChange}
                                    autoFocus
                                    required
                                />
                            </div>
                            <div className="auth-field">
                                <label htmlFor="signup-last" className="auth-label">Last name</label>
                                <input
                                    id="signup-last"
                                    type="text"
                                    name="last_name"
                                    className="auth-input"
                                    placeholder="Agarwal"
                                    value={formData.last_name}
                                    onChange={handleChange}
                                    required
                                />
                            </div>
                        </div>

                        <div className="auth-field">
                            <label htmlFor="signup-email" className="auth-label">Email address</label>
                            <input
                                id="signup-email"
                                type="email"
                                name="email"
                                className="auth-input"
                                placeholder="you@example.com"
                                value={formData.email}
                                onChange={handleChange}
                                required
                            />
                        </div>

                        <div className="auth-field">
                            <label htmlFor="signup-password" className="auth-label">Password</label>
                            <input
                                id="signup-password"
                                type="password"
                                name="password"
                                className="auth-input"
                                placeholder="Min. 6 characters"
                                value={formData.password}
                                onChange={handleChange}
                                required
                            />
                        </div>

                        <div className="auth-field">
                            <label htmlFor="signup-confirm" className="auth-label">Confirm password</label>
                            <input
                                id="signup-confirm"
                                type="password"
                                name="confirm_password"
                                className="auth-input"
                                placeholder="Re-enter your password"
                                value={formData.confirm_password}
                                onChange={handleChange}
                                required
                            />
                        </div>

                        <button
                            id="signup-submit-btn"
                            type="submit"
                            className={`auth-btn ${loading ? 'auth-btn--loading' : ''}`}
                            disabled={loading}
                        >
                            {loading ? (
                                <>
                                    <span className="auth-spinner" />
                                    Creating account…
                                </>
                            ) : 'Create Account'}
                        </button>
                    </form>

                    <p className="auth-switch-text">
                        Already have an account?{' '}
                        <button className="auth-link-btn" onClick={onSwitchToSignIn} id="goto-signin-btn">
                            Sign in
                        </button>
                    </p>
                </div>
            </div>
        </div>
    );
}
