const RangeTab = ({
    rangeStartDate,
    setRangeStartDate,
    rangeEndDate,
    setRangeEndDate,
    rangeLoading,
    rangeProgress,
    exportLoading,
    exportLog,
    consolidationReady,
    exportedRange,
    dashboardLoading,
    dashboardResult,
    dashboardBatchProgress,
    handleDownloadRangeFromNSE,
    handleExportConsolidated,
    handleBuildDashboard,
    handleDownloadDashboard
}) => {
    return (
        <section className="section range-tab-section">
            <div className="section-header">
                <h2>📅 <span style={{ color: '#2d7ff9' }}>Download Market Cap Data</span> <span className="date-range-label">(Date Range)</span></h2>
                <p className="section-hint">Download multiple days of data at once. Select start and end dates, and all trading days in between will be downloaded.</p>
            </div>
            <div className="card card-main">
                <div className="form-row form-row-spaced">
                    <div className="form-group">
                        <label htmlFor="start-date">Start Date</label>
                        <input
                            id="start-date"
                            type="date"
                            value={rangeStartDate}
                            onChange={(e) => setRangeStartDate(e.target.value)}
                            className="form-input" />


                    </div>
                    <div className="form-group">
                        <label htmlFor="end-date">End Date</label>
                        <input
                            id="end-date"
                            type="date"
                            value={rangeEndDate}
                            onChange={(e) => setRangeEndDate(e.target.value)}
                            className="form-input"
                            disabled={rangeLoading} />
                    </div>
                </div>

                <div className="info-box info-box-highlight" style={{ marginBottom: '24px' }}>
                    <span className="info-icon">ℹ️</span>
                    <span>
                        <strong style={{ color: '#1bb76e' }}>All trading days between selected dates (excluding weekends) will be downloaded.</strong><br />
                        Files saved as <span className="filename">mcapDDMMYYYY.csv</span> in <span className="folder">Backend/nosubject/</span>.
                    </span>
                </div>

                <div className="action-row action-row-spaced" style={{ display: 'flex', gap: '18px', marginBottom: '24px' }}>
                    <button
                        className="btn btn-secondary btn-large"
                        style={{ background: '#ffe0b2', color: '#176a3a', fontWeight: 'bold', borderRadius: '12px', boxShadow: '0 2px 12px rgba(44,62,80,0.10)' }}
                        onClick={handleDownloadRangeFromNSE}
                        disabled={rangeLoading || !rangeStartDate || !rangeEndDate}
                        title="Download and save all CSVs to backend storage"
                    >
                        <span style={{ fontSize: '1.3rem' }}>⬇️</span> {rangeLoading ? 'Downloading...' : 'Download Range'}
                    </button>
                    <button
                        className="btn btn-success btn-large"
                        style={{ background: '#b2dfdb', color: '#176a3a', fontWeight: 'bold', borderRadius: '12px', boxShadow: '0 2px 12px rgba(44,62,80,0.10)' }}
                        onClick={() => handleExportConsolidated('range')}
                        disabled={exportLoading || !rangeStartDate || !rangeEndDate}
                        title="Build Excel (MCAP + PR) from saved CSVs in the selected range"
                    >
                        <span style={{ fontSize: '1.3rem' }}>📑</span> {exportLoading ? 'Exporting...' : 'Export Range Excel'}
                    </button>
                    <button
                        className="btn btn-outline btn-large"
                        style={{ background: '#e0f2f1', color: '#176a3a', fontWeight: 'bold', borderRadius: '12px', boxShadow: '0 2px 12px rgba(44,62,80,0.10)' }}
                        onClick={handleBuildDashboard}
                        disabled={dashboardLoading || !consolidationReady || !rangeStartDate || !rangeEndDate}
                        title={!rangeStartDate || !rangeEndDate ? "Select date range first" : (consolidationReady ? "Build symbol dashboard for top 1000 companies by Market Cap average" : "Export Excel first to calculate MCAP & PR averages")}
                    >
                        <span style={{ fontSize: '1.3rem' }}>📊</span> {dashboardLoading ? 'Building...' : consolidationReady ? 'Build Dashboard' : 'Build Dashboard (Export First)'}
                    </button>
                </div>

                {/* Dashboard Building Progress Indicator */}
                {dashboardLoading && (
                    <div style={{ 
                        background: 'linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%)', 
                        borderRadius: '16px', 
                        padding: '24px 28px', 
                        marginBottom: '24px',
                        border: '2px solid #81c784',
                        boxShadow: '0 4px 20px rgba(76, 175, 80, 0.15)'
                    }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '16px', marginBottom: '16px' }}>
                            <div style={{
                                width: '40px',
                                height: '40px',
                                border: '4px solid #e0e0e0',
                                borderTop: '4px solid #4caf50',
                                borderRadius: '50%',
                                animation: 'spin 1s linear infinite'
                            }} />
                            <div>
                                <div style={{ fontWeight: 'bold', fontSize: '1.2rem', color: '#2e7d32' }}>
                                    🔄 Building Dashboard...
                                </div>
                                <div style={{ color: '#558b2f', fontSize: '0.95rem', marginTop: '4px' }}>
                                    {dashboardBatchProgress 
                                        ? `Batch ${dashboardBatchProgress.currentBatch}/${dashboardBatchProgress.totalBatches}: ${dashboardBatchProgress.status}`
                                        : 'Initializing...'}
                                </div>
                            </div>
                        </div>
                        
                        {/* Progress Bar */}
                        {dashboardBatchProgress && (
                            <div style={{ marginBottom: '16px' }}>
                                <div style={{ 
                                    background: '#fff', 
                                    borderRadius: '10px', 
                                    height: '24px',
                                    overflow: 'hidden',
                                    border: '1px solid #a5d6a7'
                                }}>
                                    <div style={{
                                        background: 'linear-gradient(90deg, #4caf50, #81c784)',
                                        height: '100%',
                                        width: `${(dashboardBatchProgress.symbolsProcessed / dashboardBatchProgress.totalSymbols) * 100}%`,
                                        transition: 'width 0.5s ease',
                                        display: 'flex',
                                        alignItems: 'center',
                                        justifyContent: 'center',
                                        color: '#fff',
                                        fontWeight: 'bold',
                                        fontSize: '0.85rem'
                                    }}>
                                        {Math.round((dashboardBatchProgress.symbolsProcessed / dashboardBatchProgress.totalSymbols) * 100)}%
                                    </div>
                                </div>
                                <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '8px', fontSize: '0.9rem', color: '#558b2f' }}>
                                    <span>{dashboardBatchProgress.symbolsProcessed} symbols processed</span>
                                    <span>{dashboardBatchProgress.totalSymbols} total</span>
                                </div>
                            </div>
                        )}
                        
                        <div style={{ 
                            background: '#fff', 
                            borderRadius: '10px', 
                            padding: '14px 18px',
                            border: '1px solid #a5d6a7'
                        }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                                <span style={{ color: '#666', fontWeight: '500' }}>📦 Batch:</span>
                                <span style={{ fontWeight: 'bold', color: '#2e7d32' }}>
                                    {dashboardBatchProgress ? `${dashboardBatchProgress.currentBatch} of ${dashboardBatchProgress.totalBatches}` : '—'}
                                </span>
                            </div>
                            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                                <span style={{ color: '#666', fontWeight: '500' }}>🔢 Symbols per batch:</span>
                                <span style={{ fontWeight: 'bold', color: '#2e7d32' }}>100</span>
                            </div>
                            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                                <span style={{ color: '#666', fontWeight: '500' }}>⏱️ Est. per batch:</span>
                                <span style={{ fontWeight: 'bold', color: '#2e7d32' }}>~20-25 seconds</span>
                            </div>
                        </div>
                        <div style={{ marginTop: '12px', fontSize: '0.9rem', color: '#689f38', textAlign: 'center' }}>
                            💡 <em>Each batch request completes within Render's 30s limit</em>
                        </div>
                    </div>
                )}

                {consolidationReady ? (
                    <div className="consolidation-details" style={{ marginBottom: '18px' }}>
                        <span className="pill pill-success" style={{ background: '#1bb76e', color: '#fff', borderRadius: '8px', padding: '4px 12px', marginRight: '8px' }}>Dashboard Ready</span>
                        <span className="consolidation-hint" style={{ color: '#176a3a' }}>Top 1000 companies by MCAP average</span>
                    </div>
                ) : (
                    <div style={{ background: '#fff8e1', borderRadius: '12px', padding: '18px 24px', marginTop: '12px', border: '1px solid #ffe0b2', marginBottom: '18px' }}>
                        <span style={{ color: '#f9a825', fontWeight: 'bold', fontSize: '1.08rem' }}><span style={{ fontSize: '1.2rem' }}>⚠️</span> To build dashboard:</span>
                        <ol className="consolidation-steps" style={{ marginTop: '12px', marginBottom: '0', paddingLeft: '18px' }}>
                            <li style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 'bold', color: rangeStartDate && rangeEndDate ? '#1bb76e' : '#888' }}>
                                {rangeStartDate && rangeEndDate && <span style={{ fontSize: '1.2rem', color: '#1bb76e' }}>✔️</span>}
                                1. Select date range
                            </li>
                            <li style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 'bold', color: rangeProgress ? '#1bb76e' : '#888' }}>
                                {rangeProgress && <span style={{ fontSize: '1.2rem', color: '#1bb76e' }}>✔️</span>}
                                2. Download Range (fetch MCAP/PR files)
                            </li>
                            <li style={{ fontWeight: 'bold', color: '#f9a825' }}>3. Export Range Excel (calculates averages)</li>
                        </ol>
                    </div>
                )}
                {/* ...existing code... */}
            </div>

            {rangeProgress && (
                <div className="scrape-session-panel card card-subtle frosted-bg" style={{ background: 'rgba(255,255,255,0.7)', borderRadius: '24px', boxShadow: '0 4px 24px rgba(44,62,80,0.10)', padding: '32px 24px', margin: '0 0 32px 0', backdropFilter: 'blur(8px)' }}>
                    <h3 style={{ marginBottom: '2rem', fontWeight: '600', fontSize: '1.3rem', color: '#2d7ff9' }}>
                        <span role="img" aria-label="summary">📋</span> Range Download Summary
                    </h3>
                    <div className="progress-stats modern-stats" style={{ display: 'flex', gap: '18px', marginBottom: '2rem' }}>
                        <div className="stat success" style={{ background: '#e6f9ed', borderRadius: '12px', padding: '18px 0', flex: '1', textAlign: 'center' }}><span className="stat-label" style={{ color: '#1bb76e', fontWeight: 'bold' }}>CACHED</span><br /><span className="stat-value" style={{ fontSize: '2rem', fontWeight: 'bold' }}>{rangeProgress.summary.cached}</span></div>
                        <div className="stat info" style={{ background: '#e6f0fa', borderRadius: '12px', padding: '18px 0', flex: '1', textAlign: 'center' }}><span className="stat-label" style={{ color: '#2d7ff9', fontWeight: 'bold' }}>FETCHED</span><br /><span className="stat-value" style={{ fontSize: '2rem', fontWeight: 'bold' }}>{rangeProgress.summary.fetched}</span></div>
                        <div className="stat failed" style={{ background: '#fae6e6', borderRadius: '12px', padding: '18px 0', flex: '1', textAlign: 'center' }}><span className="stat-label" style={{ color: '#e74c3c', fontWeight: 'bold' }}>FAILED</span><br /><span className="stat-value" style={{ fontSize: '2rem', fontWeight: 'bold' }}>{rangeProgress.summary.failed}</span></div>
                        <div className="stat total" style={{ background: '#f7f7f7', borderRadius: '12px', padding: '18px 0', flex: '1', textAlign: 'center' }}><span className="stat-label" style={{ color: '#f39c12', fontWeight: 'bold' }}>TOTAL</span><br /><span className="stat-value" style={{ fontSize: '2rem', fontWeight: 'bold' }}>{rangeProgress.summary.total_requested}</span></div>
                    </div>
                    {rangeProgress.summary.fetched === 0 && rangeProgress.summary.failed === 0 && (
                        <div className="pill pill-success" style={{ marginBottom: '1rem' }}>All days served from cache</div>
                    )}
                    {rangeProgress.entries && rangeProgress.entries.length > 0 && (
                        <div className="range-status-list">
                            <div className="range-status-header" style={{ fontWeight: 'bold', margin: '1.5rem 0 1rem 0', fontSize: '1.1rem', color: '#2d7ff9' }}>Per-day status</div>
                            <div className="range-status-cards-grid" style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '20px' }}>
                                {(() => {
                                    // Group entries by date
                                    const grouped = {};
                                    rangeProgress.entries.forEach(entry => {
                                        if (!grouped[entry.date]) grouped[entry.date] = {};
                                        grouped[entry.date][entry.type.toUpperCase()] = entry;
                                    });
                                    return Object.entries(grouped).map(([date, types]) => (
                                        <div key={date} className="range-status-card frosted-bg" style={{ background: '#f6fff8', borderRadius: '16px', boxShadow: '0 2px 12px rgba(44,62,80,0.08)', padding: '18px 16px', display: 'flex', flexDirection: 'column', alignItems: 'flex-start', minHeight: '120px', justifyContent: 'center' }}>
                                            <div className="range-status-date" style={{ fontWeight: 'bold', fontSize: '1.1rem', color: '#176a3a', marginBottom: '10px', letterSpacing: '1px' }}>{date}</div>
                                            <div style={{ display: 'flex', gap: '12px', width: '100%' }}>
                                                {['MCAP', 'PR'].map(type => (
                                                    <div key={type} className={`range-status-pill status-${types[type]?.status ?? 'none'}`}
                                                        style={{ background: '#e6f9ed', borderRadius: '10px', flex: '1', padding: '10px 12px', boxShadow: '0 1px 4px rgba(44,62,80,0.04)', display: 'flex', flexDirection: 'column', alignItems: 'flex-start', gap: '2px' }}>
                                                        {types[type] ? (
                                                            <>
                                                                <span className="type-label" style={{ fontWeight: 'bold', color: '#1bb76e', fontSize: '1rem' }}>{type}</span>
                                                                <span className="status-label" style={{ color: '#1bb76e', fontWeight: '500', fontSize: '0.95rem' }}>{types[type].status.toLowerCase()}</span>
                                                                {types[type].records !== undefined && <span className="records-label" style={{ color: '#888', fontSize: '0.92rem' }}>{types[type].records} records</span>}
                                                            </>
                                                        ) : <span style={{ color: '#ccc' }}>—</span>}
                                                    </div>
                                                ))}
                                            </div>
                                        </div>
                                    ));
                                })()}
                            </div>
                        </div>
                    )}
                    {rangeProgress.errors && rangeProgress.errors.length > 0 && (
                        <div className="errors-list" style={{ marginTop: '2rem' }}>
                            <div style={{ fontWeight: 'bold', fontSize: '1.1rem', color: '#1bb76e', marginBottom: '0.5rem' }}>Failed Downloads:</div>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                                {rangeProgress.errors.map((err, idx) => (
                                    <div key={idx} className="error-item" style={{ background: '#fae6e6', borderRadius: '12px', padding: '12px 18px', display: 'flex', alignItems: 'center', gap: '12px' }}>
                                        <span style={{ fontSize: '1.5rem', color: '#e74c3c' }}>❌</span>
                                        <span style={{ color: '#e74c3c', fontWeight: 'bold' }}>{err.date}</span>
                                        <span style={{ color: '#e74c3c' }}>-</span>
                                        <span style={{ color: '#e74c3c' }}>{err.error}</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            )}

            {dashboardResult && (
                <div className="dashboard-panel card card-subtle" style={{ marginBottom: '24px', padding: '28px 32px', borderRadius: '18px', boxShadow: '0 2px 12px rgba(44,62,80,0.08)' }}>
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '18px' }}>
                        <h3 style={{ fontWeight: '700', fontSize: '1.35rem', margin: '0', display: 'flex', alignItems: 'center' }}>
                            <span style={{ fontSize: '1.5rem', marginRight: '10px' }}>📊</span> Symbol Dashboard
                        </h3>
                        <span style={{ background: '#f5faff', color: '#2d7ff9', border: '2px solid #e0e7ff', borderRadius: '18px', padding: '6px 18px', fontWeight: 'bold', fontSize: '1rem' }}>
                            {dashboardResult.symbols_used || dashboardResult.count || 0} symbols
                        </span>
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: '18px', marginBottom: '18px' }}>
                        <div style={{ background: '#eafaf3', borderRadius: '12px', padding: '18px', textAlign: 'center', border: '2px solid #1bb76e22' }}>
                            <div style={{ fontWeight: '600', color: '#7b8a8b', fontSize: '1.05rem', marginBottom: '8px' }}>IMPACT COST (AVG)</div>
                            <div style={{ fontWeight: 'bold', fontSize: '2rem', color: '#222' }}>{dashboardResult.averages?.impact_cost ?? 'N/A'}</div>
                        </div>
                        <div style={{ background: '#eafaf3', borderRadius: '12px', padding: '18px', textAlign: 'center', border: '2px solid #1bb76e22' }}>
                            <div style={{ fontWeight: '600', color: '#7b8a8b', fontSize: '1.05rem', marginBottom: '8px' }}>FREE FLOAT MCAP (AVG)</div>
                            <div style={{ fontWeight: 'bold', fontSize: '2rem', color: '#222' }}>{dashboardResult.averages?.free_float_mcap ?? 'N/A'}</div>
                        </div>
                        <div style={{ background: '#eafaf3', borderRadius: '12px', padding: '18px', textAlign: 'center', border: '2px solid #1bb76e22' }}>
                            <div style={{ fontWeight: '600', color: '#7b8a8b', fontSize: '1.05rem', marginBottom: '8px' }}>TOTAL MCAP (AVG)</div>
                            <div style={{ fontWeight: 'bold', fontSize: '2rem', color: '#222' }}>{dashboardResult.averages?.total_market_cap ?? 'N/A'}</div>
                        </div>
                        <div style={{ background: '#eafaf3', borderRadius: '12px', padding: '18px', textAlign: 'center', border: '2px solid #1bb76e22' }}>
                            <div style={{ fontWeight: '600', color: '#7b8a8b', fontSize: '1.05rem', marginBottom: '8px' }}>TRADED VALUE (AVG)</div>
                            <div style={{ fontWeight: 'bold', fontSize: '2rem', color: '#222' }}>{dashboardResult.averages?.total_traded_value ?? 'N/A'}</div>
                        </div>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
                        <button
                            className="btn btn-warning btn-large"
                            style={{ background: '#ffa726', color: '#fff', fontWeight: 'bold', borderRadius: '10px', fontSize: '1.08rem', padding: '12px 32px', boxShadow: '0 2px 8px rgba(44,62,80,0.08)' }}
                            onClick={handleDownloadDashboard}
                            disabled={!dashboardResult.download_url}
                        >
                            <span style={{ fontSize: '1.2rem', marginRight: '8px' }}>⬇️</span> Download Dashboard Excel
                        </button>
                        {dashboardResult.errors && dashboardResult.errors.length > 0 && (
                            <span style={{ background: '#ffeaea', color: '#e74c3c', borderRadius: '18px', padding: '8px 18px', fontWeight: 'bold', fontSize: '1rem', border: '2px solid #ffd6d6' }}>
                                {dashboardResult.errors.length} symbols failed
                            </span>
                        )}
                    </div>
                </div>
            )}

            <div className="download-info card card-subtle">
                <h4>How it works:</h4>
                <ol style={{ marginTop: '10px', marginBottom: '0', paddingLeft: '18px' }}>
                    <li style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 'bold', color: '#1bb76e' }}>
                        <span style={{ fontSize: '1.2rem', color: '#1bb76e' }}>✔️</span> Select start date (e.g., 01-Dec-2025)
                    </li>
                    <li style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 'bold', color: '#1bb76e' }}>
                        <span style={{ fontSize: '1.2rem', color: '#1bb76e' }}>✔️</span> Select end date (e.g., 05-Dec-2025)
                    </li>
                    <li style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 'bold', color: '#1bb76e' }}>
                        <span style={{ fontSize: '1.2rem', color: '#1bb76e' }}>✔️</span> Click <span className="filename">⬇️ Download Range</span>
                    </li>
                    <li style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 'bold', color: '#1bb76e' }}>
                        <span style={{ fontSize: '1.2rem', color: '#1bb76e' }}>✔️</span> All trading days between dates are downloaded automatically
                    </li>
                    <li style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 'bold', color: '#1bb76e' }}>
                        <span style={{ fontSize: '1.2rem', color: '#1bb76e' }}>✔️</span> Files saved with pattern: <span className="filename">mcapDDMMYYYY.csv</span>
                    </li>
                    <li style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 'bold', color: '#1bb76e' }}>
                        <span style={{ fontSize: '1.2rem', color: '#1bb76e' }}>✔️</span> Go to <span className="filename">Upload & Process</span> to consolidate all downloaded files
                    </li>
                </ol>
            </div>
        </section>
    );
};

export default RangeTab;
