import React from 'react';
import './Header.css';

const Header = () => {
    return (
        <header className="header">
            <div className="header-content">
                <div className="header-icon">
                    <svg
                        width="48"
                        height="48"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                    >
                        <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline>
                    </svg>
                </div>
                <div className="header-text">
                    <h1 className="header-title">Market Cap Consolidation Tool</h1>
                    <p className="header-subtitle">Professional market capitalization data consolidation made simple</p>
                </div>
            </div>
        </header>
    );
};

export default Header;
