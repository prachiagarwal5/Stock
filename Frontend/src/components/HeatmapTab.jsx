import React from 'react';

const HEATMAP_INDICES = [
    'NIFTY 50',
    'NIFTY NEXT 50',
    'NIFTY MIDCAP 50',
    'NIFTY MIDCAP 100',
    'NIFTY MIDCAP 150',
    'NIFTY SMALLCAP 50',
    'NIFTY SMALLCAP 100',
    'NIFTY SMALLCAP 250',
    'NIFTY MIDSMALLCAP 400',
    'NIFTY 100',
    'NIFTY 200',
    'NIFTY500 MULTICAP 50:25:25',
    'NIFTY LARGEMIDCAP 250',
    'NIFTY MIDCAP SELECT'
];

const getHeatmapColor = (pChange) => {
    if (pChange === null || pChange === undefined || Number.isNaN(pChange)) return '#e5e7eb';
    const val = Number(pChange);
    if (val >= 3.5) return '#087f3f';
    if (val >= 2.5) return '#0b9950';
    if (val >= 1.5) return '#16a34a';
    if (val >= 0.5) return '#34d399';
    if (val > 0) return '#b7e4c7';
    if (val <= -3.5) return '#b91c1c';
    if (val <= -2.5) return '#dc2626';
    if (val <= -1.5) return '#ef4444';
    if (val <= -0.5) return '#f87171';
    if (val < 0) return '#fecdd3';
    return '#e5e7eb';
};

const formatPrice = (value) => {
    if (value === null || value === undefined || Number.isNaN(value)) return '—';
    return Number(value).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};

const StockPopup = ({ selectedStock, popupPosition, setSelectedStock }) => {
    if (!selectedStock) return null;

    return (
        <div className="stock-popup-overlay" onClick={() => setSelectedStock(null)}>
            <div
                className="stock-popup"
                onClick={(e) => e.stopPropagation()}
                style={{
                    position: 'fixed',
                    left: Math.min(popupPosition.x, window.innerWidth - 320),
                    top: Math.max(popupPosition.y - 10, 60)
                }}
            >
                <div className="stock-popup-header">
                    <span className="stock-popup-symbol">{selectedStock.symbol}</span>
                    <div className="stock-popup-tabs">
                        <span className="stock-popup-tab active">Price</span>
                        <span className="stock-popup-tab">Graph</span>
                    </div>
                </div>
                <div className="stock-popup-body">
                    <div className="stock-popup-row">
                        <span className="stock-popup-label">Change</span>
                        <span className={`stock-popup-value ${Number(selectedStock.change) >= 0 ? 'positive' : 'negative'}`}>
                            {selectedStock.change ?? '—'}
                        </span>
                    </div>
                    <div className="stock-popup-row">
                        <span className="stock-popup-label">VWAP</span>
                        <span className="stock-popup-value">{selectedStock.vwap ?? '—'}</span>
                    </div>
                    <div className="stock-popup-row">
                        <span className="stock-popup-label">High</span>
                        <span className="stock-popup-value">{selectedStock.high ?? selectedStock.dayHigh ?? '—'}</span>
                    </div>
                    <div className="stock-popup-row">
                        <span className="stock-popup-label">Low</span>
                        <span className="stock-popup-value">{selectedStock.low ?? selectedStock.dayLow ?? '—'}</span>
                    </div>
                    <div className="stock-popup-row">
                        <span className="stock-popup-label">Traded Volume (Lakhs)</span>
                        <span className="stock-popup-value">
                            {selectedStock.totalTradedVolume
                                ? (Number(selectedStock.totalTradedVolume) / 100000).toFixed(2)
                                : '—'}
                        </span>
                    </div>
                    <div className="stock-popup-row">
                        <span className="stock-popup-label">Traded Value (Cr.)</span>
                        <span className="stock-popup-value">
                            {selectedStock.totalTradedValue
                                ? (Number(selectedStock.totalTradedValue) / 10000000).toFixed(2)
                                : '—'}
                        </span>
                    </div>
                </div>
                <button className="stock-popup-close" onClick={() => setSelectedStock(null)}>×</button>
            </div>
        </div>
    );
};

const HeatmapTab = ({
    heatmapIndex,
    setHeatmapIndex,
    heatmapData,
    heatmapMeta,
    heatmapLoading,
    heatmapError,
    fetchHeatmapData,
    selectedStock,
    setSelectedStock,
    popupPosition,
    setPopupPosition
}) => {
    return (
        <section className="section heatmap-section">
            <div className="section-header">
                <div>
                    <h2>🟩 NSE Heatmap</h2>
                    <p className="section-hint">Live price movers by index, similar to the NSE heatmap view</p>
                </div>
            </div>

            {heatmapError && <div className="alert alert-error">{heatmapError}</div>}

            <div className="heatmap-layout">
                <aside className="heatmap-sidebar">
                    <div className="heatmap-sidebar-title">Indices</div>
                    <div className="heatmap-index-list">
                        {HEATMAP_INDICES.map((idx) => (
                            <button
                                key={idx}
                                className={`heatmap-index-btn ${heatmapIndex === idx ? 'active' : ''}`}
                                onClick={() => setHeatmapIndex(idx)}
                                disabled={heatmapLoading && heatmapIndex === idx}
                            >
                                {idx}
                            </button>
                        ))}
                    </div>
                    <button
                        className="btn btn-outline heatmap-refresh"
                        onClick={() => fetchHeatmapData(heatmapIndex)}
                        disabled={heatmapLoading}
                    >
                        {heatmapLoading ? '⏳ Loading...' : '🔄 Refresh'}
                    </button>
                </aside>

                <div className="heatmap-content">
                    <div className="heatmap-meta">
                        <div>
                            <h3>{heatmapMeta?.index || heatmapIndex}</h3>
                            <p className="section-hint">As on {heatmapMeta?.timestamp || '—'}</p>
                        </div>
                        <div className="heatmap-advances">
                            <span className="pill pill-success">Advances {heatmapMeta?.advances?.advances ?? 0}</span>
                            <span className="pill pill-warning">Declines {heatmapMeta?.advances?.declines ?? 0}</span>
                            <span className="pill pill-info">Unchanged {heatmapMeta?.advances?.unchanged ?? 0}</span>
                        </div>
                    </div>

                    {heatmapLoading && (
                        <div className="heatmap-loading">Building heatmap...</div>
                    )}

                    {!heatmapLoading && heatmapData.length === 0 && (
                        <div className="heatmap-empty">No symbols available for this index.</div>
                    )}

                    {!heatmapLoading && heatmapData.length > 0 && (
                        <div className="heatmap-grid">
                            {heatmapData.map((row, idx) => {
                                const change = row.pChange ?? row.perChange ?? row.change;
                                const bg = getHeatmapColor(change);
                                const textColor = Math.abs(Number(change) || 0) < 0.5 ? '#0f172a' : '#ffffff';
                                return (
                                    <div
                                        key={`${row.symbol || row.symbolName || row.identifier || idx}-${idx}`}
                                        className="heatmap-card"
                                        style={{ backgroundColor: bg, color: textColor, cursor: 'pointer' }}
                                        onClick={(e) => {
                                            const rect = e.currentTarget.getBoundingClientRect();
                                            setPopupPosition({
                                                x: rect.left + rect.width / 2,
                                                y: rect.top
                                            });
                                            setSelectedStock(row);
                                        }}
                                    >
                                        <div className="heatmap-symbol">{row.symbol || row.symbolName || row.identifier}</div>
                                        <div className="heatmap-price">{formatPrice(row.lastPrice ?? row.last)}</div>
                                        <div className="heatmap-change">{change === null || change === undefined || Number.isNaN(change) ? '—' : `${Number(change).toFixed(2)}%`}</div>
                                    </div>
                                );
                            })}
                        </div>
                    )}

                    <StockPopup
                        selectedStock={selectedStock}
                        popupPosition={popupPosition}
                        setSelectedStock={setSelectedStock}
                    />
                </div>
            </div>
        </section>
    );
};

export default HeatmapTab;
