import React, { useState } from 'react';

const Header = () => {
    const [refreshing, setRefreshing] = useState(false);
    const [indexStatus, setIndexStatus] = useState(null);
    const [showStatus, setShowStatus] = useState(false);

    const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000';

    const handleRefreshIndices = async () => {
        setRefreshing(true);
        setShowStatus(false);

        try {
            const response = await fetch(`${API_URL}/api/nifty-indices/fetch-and-store`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            if (!response.ok) {
                throw new Error(`Failed to refresh indices: ${response.status}`);
            }

            const data = await response.json();
            setIndexStatus({
                type: 'success',
                message: `✓ Successfully updated ${data.total_symbols} symbols`,
                details: data.index_distribution
            });
            setShowStatus(true);

            // Auto-hide after 5 seconds
            setTimeout(() => {
                setShowStatus(false);
            }, 5000);

        } catch (error) {
            console.error('Error refreshing indices:', error);
            setIndexStatus({
                type: 'error',
                message: `✗ Failed to refresh indices: ${error.message}`
            });
            setShowStatus(true);

            // Auto-hide after 7 seconds
            setTimeout(() => {
                setShowStatus(false);
            }, 7000);
        } finally {
            setRefreshing(false);
        }
    };

    const checkIndexStatus = async () => {
        try {
            const response = await fetch(`${API_URL}/api/nifty-indices/status`);
            if (response.ok) {
                const data = await response.json();
                return data;
            }
        } catch (error) {
            console.error('Error checking index status:', error);
        }
        return null;
    };

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
                <div className="header-actions">
                    <button
                        className={`refresh-indices-btn ${refreshing ? 'loading' : ''}`}
                        onClick={handleRefreshIndices}
                        disabled={refreshing}
                        title="Refresh Nifty index data from NSE"
                    >
                        <svg
                            width="18"
                            height="18"
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="currentColor"
                            strokeWidth="2"
                            className={refreshing ? 'spinning' : ''}
                        >
                            <polyline points="23 4 23 10 17 10"></polyline>
                            <polyline points="1 20 1 14 7 14"></polyline>
                            <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
                        </svg>
                        {refreshing ? 'Updating...' : 'Refresh Indices'}
                    </button>
                </div>
            </div>

            {showStatus && indexStatus && (
                <div className={`index-status-banner ${indexStatus.type}`}>
                    <div className="status-message">
                        {indexStatus.message}
                    </div>
                    {indexStatus.details && (
                        <div className="status-details">
                            {Object.entries(indexStatus.details).map(([index, count]) => (
                                <span key={index} className="status-detail-item">
                                    {index}: {count}
                                </span>
                            ))}
                        </div>
                    )}
                    <button
                        className="status-close"
                        onClick={() => setShowStatus(false)}
                        aria-label="Close"
                    >
                        ×
                    </button>
                </div>
            )}
        </header>
    );
};

export default Header;
