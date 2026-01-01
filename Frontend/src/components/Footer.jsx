import React from 'react';

const Footer = () => {
    return (
        <footer className="footer">
            <div className="footer-content">
                <p>
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ verticalAlign: 'middle', marginRight: '8px' }}>
                        <rect x="2" y="7" width="20" height="14" rx="2" ry="2"></rect>
                        <path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"></path>
                    </svg>
                    Market Cap Consolidation Tool
                    <span className="footer-separator">|</span>
                    Powered by React & Flask
                </p>
                <p className="footer-copyright">© 2025 All rights reserved</p>
            </div>
        </footer>
    );
};

export default Footer;
