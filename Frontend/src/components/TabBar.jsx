import React, { useState } from 'react';

const TabBar = ({ activeTab, setActiveTab }) => {
    const [ripple, setRipple] = useState({ show: false, x: 0, y: 0, key: 0 });

    const handleClick = (e, tab) => {
        const rect = e.currentTarget.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        
        setRipple({ show: true, x, y, key: Date.now() });
        setActiveTab(tab);
        
        setTimeout(() => setRipple(prev => ({ ...prev, show: false })), 700);
    };

    const tabs = [
        {
            id: 'download',
            label: 'Download NSE',
            icon: (
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                    <polyline points="7 10 12 15 17 10"></polyline>
                    <line x1="12" y1="15" x2="12" y2="3"></line>
                </svg>
            )
        },
        {
            id: 'range',
            label: 'Date Range',
            icon: (
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <rect x="3" y="4" width="18" height="18" rx="2" ry="2"></rect>
                    <line x1="16" y1="2" x2="16" y2="6"></line>
                    <line x1="8" y1="2" x2="8" y2="6"></line>
                    <line x1="3" y1="10" x2="21" y2="10"></line>
                </svg>
            )
        },
        {
            id: 'heatmap',
            label: 'Heatmap',
            icon: (
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <rect x="3" y="3" width="7" height="7"></rect>
                    <rect x="14" y="3" width="7" height="7"></rect>
                    <rect x="14" y="14" width="7" height="7"></rect>
                    <rect x="3" y="14" width="7" height="7"></rect>
                </svg>
            )
        },
        {
            id: 'upload',
            label: 'Upload',
            icon: (
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                    <polyline points="17 8 12 3 7 8"></polyline>
                    <line x1="12" y1="3" x2="12" y2="15"></line>
                </svg>
            )
        },
        {
            id: 'preview',
            label: 'Preview',
            icon: (
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
                    <circle cx="12" cy="12" r="3"></circle>
                </svg>
            )
        },
        {
            id: 'mongo',
            label: 'Mongo DB',
            icon: (
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
                    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
                    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
                </svg>
            )
        }
    ];

    return (
        <aside className="sidebar">
            <div className="sidebar-header">
                <div className="sidebar-brand">
                    <div className="sidebar-logo">
                        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                            <line x1="12" y1="20" x2="12" y2="10"></line>
                            <line x1="18" y1="20" x2="18" y2="4"></line>
                            <line x1="6" y1="20" x2="6" y2="16"></line>
                        </svg>
                    </div>
                    <span className="sidebar-title">Dashboard</span>
                </div>
            </div>
            <nav className="sidebar-nav">
                {tabs.map((tab) => (
                    <button
                        key={tab.id}
                        className={`sidebar-item ${activeTab === tab.id ? 'active' : ''}`}
                        onClick={(e) => handleClick(e, tab.id)}
                    >
                        <span className="sidebar-icon">{tab.icon}</span>
                        <span className="sidebar-label">{tab.label}</span>
                        <span className="sidebar-indicator"></span>
                        {ripple.show && activeTab === tab.id && (
                            <span 
                                key={ripple.key}
                                className="ripple" 
                                style={{ left: ripple.x, top: ripple.y }}
                            />
                        )}
                    </button>
                ))}
            </nav>
            <div className="sidebar-footer">
                <div className="sidebar-footer-content">
                    <div className="sidebar-status">
                        <span className="status-dot"></span>
                        <span className="status-text">System Online</span>
                    </div>
                    <div className="sidebar-version">v2.0</div>
                </div>
            </div>
        </aside>
    );
};

export default TabBar;
