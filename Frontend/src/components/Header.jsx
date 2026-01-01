import React from 'react';

const Header = () => {
    return (
        <header className="header">
            <div className="header-content">
                <div className="header-icon">
                    <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <line x1="12" y1="20" x2="12" y2="10"></line>
                        <line x1="18" y1="20" x2="18" y2="4"></line>
                        <line x1="6" y1="20" x2="6" y2="16"></line>
                    </svg>
                </div>
                <div className="header-text">
                    <h1>Market Cap Consolidation Tool</h1>
                    <p>Upload CSV files and consolidate market cap data into a professional Excel file</p>
                </div>
            </div>
        </header>
    );
};

export default Header;
