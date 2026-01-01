import React from 'react';

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
    handleDownloadRangeFromNSE,
    handleExportConsolidated,
    handleBuildDashboard,
    handleDownloadDashboard
}) => {
    return (
        <section className="section">
            <h2>📅 Download Market Cap Data - Date Range</h2>
            <p className="section-hint">
                Download multiple days of data at once. Select start and end dates, and all trading days in between will be downloaded.
            </p>

            <div className="nse-download-panel">
                <div className="form-row">
                    <div className="form-group">
                        <label>Start Date</label>
                        <input
                            type="date"
                            value={rangeStartDate}
                            onChange={(e) => setRangeStartDate(e.target.value)}
                            className="form-input"
                            disabled={rangeLoading}
                        />
                    </div>

                    <div className="form-group">
                        <label>End Date</label>
                        <input
                            type="date"
                            value={rangeEndDate}
                            onChange={(e) => setRangeEndDate(e.target.value)}
                            className="form-input"
                            disabled={rangeLoading}
                        />
                    </div>
                </div>

                <div className="form-group">
                    <p className="info-box">
                        <span className="info-icon">ℹ️</span>
                        This will download market cap data for all trading days between the selected dates (excluding weekends).
                        Each file will be saved as <strong>mcapDDMMYYYY.csv</strong> in the Backend/nosubject/ folder.
                    </p>
                </div>

                <button
                    className="btn btn-secondary btn-large"
                    onClick={handleDownloadRangeFromNSE}
                    disabled={rangeLoading || !rangeStartDate || !rangeEndDate}
                    title="Download and save all CSVs to backend storage"
                >
                    {rangeLoading ? '⏳ Downloading...' : '⬇️ Download Range'}
                </button>

                <button
                    className="btn btn-success btn-large"
                    onClick={() => handleExportConsolidated('range')}
                    disabled={exportLoading || !rangeStartDate || !rangeEndDate}
                    title="Build Excel (MCAP + PR) from saved CSVs in the selected range"
                >
                    {exportLoading ? '⏳ Exporting...' : '📑 Export Range Excel'}
                </button>

                {exportLog.length > 0 && (
                    <div className="log-panel">
                        <h4>Export progress</h4>
                        <ul>
                            {exportLog.map((line, idx) => (
                                <li key={idx}>{line}</li>
                            ))}
                        </ul>
                    </div>
                )}

                <div className={`consolidation-status ${consolidationReady ? 'ready' : 'pending'}`}>
                    {consolidationReady ? (
                        <div>
                            <span>✅ Averages calculated for {exportedRange?.start} to {exportedRange?.end}</span>
                            <div className="consolidation-details">
                                <span className="pill pill-success">Dashboard Ready</span>
                                <span className="consolidation-hint">Top 1000 companies by MCAP average</span>
                            </div>
                        </div>
                    ) : (
                        <div>
                            <span>⚠️ To build dashboard:</span>
                            <ol className="consolidation-steps">
                                <li className={rangeStartDate && rangeEndDate ? 'done' : ''}>Select date range</li>
                                <li className={rangeProgress ? 'done' : ''}>Download Range (fetch MCAP/PR files)</li>
                                <li>Export Range Excel (calculates averages)</li>
                            </ol>
                        </div>
                    )}
                </div>

                <button
                    className="btn btn-outline btn-large"
                    onClick={handleBuildDashboard}
                    disabled={dashboardLoading || !consolidationReady || !rangeStartDate || !rangeEndDate}
                    title={!rangeStartDate || !rangeEndDate ? "Select date range first" : (consolidationReady ? "Build symbol dashboard for top 1000 companies by Market Cap average" : "Export Excel first to calculate MCAP & PR averages")}
                >
                    {dashboardLoading ? '⏳ Building...' : consolidationReady ? '📊 Build Dashboard (Top 1000 by MCAP Avg)' : '📊 Build Dashboard (Export First)'}
                </button>

                {rangeProgress && (
                    <div className="scrape-session-panel">
                        <h3>📋 Range Download Summary</h3>
                        <div className="session-info">
                            <p><strong>Cached:</strong> {rangeProgress.summary.cached}</p>
                            <p><strong>Fetched:</strong> {rangeProgress.summary.fetched}</p>
                            <p><strong>Failed:</strong> {rangeProgress.summary.failed}</p>
                            <p><strong>Total Requested:</strong> {rangeProgress.summary.total_requested}</p>
                            {rangeProgress.summary.fetched === 0 && rangeProgress.summary.failed === 0 && (
                                <div className="pill pill-success">All days served from cache</div>
                            )}
                        </div>
                        {rangeProgress.entries && rangeProgress.entries.length > 0 && (
                            <div className="range-status-list">
                                <div className="range-status-header">Per-day status</div>
                                <ul>
                                    {rangeProgress.entries.map((entry, idx) => (
                                        <li key={idx} className={`range-status-item status-${entry.status}`}>
                                            <span className="date">{entry.date}</span>
                                            <span className="type">{entry.type.toUpperCase()}</span>
                                            <span className="status">{entry.status}</span>
                                            {entry.records !== undefined && <span className="records">{entry.records} records</span>}
                                        </li>
                                    ))}
                                </ul>
                            </div>
                        )}
                    </div>
                )}

                {dashboardResult && (
                    <div className="dashboard-panel">
                        <div className="dashboard-header">
                            <h3>📊 Symbol Dashboard</h3>
                            <div className="pill pill-info">{dashboardResult.symbols_used || dashboardResult.count || 0} symbols</div>
                        </div>
                        <div className="dashboard-grid">
                            <div className="stat">
                                <span className="stat-label">Impact Cost (avg)</span>
                                <span className="stat-value">{dashboardResult.averages?.impact_cost ?? 'N/A'}</span>
                            </div>
                            <div className="stat">
                                <span className="stat-label">Free Float Mcap (avg)</span>
                                <span className="stat-value">{dashboardResult.averages?.free_float_mcap ?? 'N/A'}</span>
                            </div>
                            <div className="stat">
                                <span className="stat-label">Total Mcap (avg)</span>
                                <span className="stat-value">{dashboardResult.averages?.total_market_cap ?? 'N/A'}</span>
                            </div>
                            <div className="stat">
                                <span className="stat-label">Traded Value (avg)</span>
                                <span className="stat-value">{dashboardResult.averages?.total_traded_value ?? 'N/A'}</span>
                            </div>
                        </div>
                        <div className="dashboard-actions">
                            <button
                                className="btn btn-secondary"
                                onClick={handleDownloadDashboard}
                                disabled={!dashboardResult.download_url}
                            >
                                ⬇️ Download Dashboard Excel
                            </button>
                            {dashboardResult.errors && dashboardResult.errors.length > 0 && (
                                <span className="pill pill-warning">{dashboardResult.errors.length} symbols failed</span>
                            )}
                        </div>
                    </div>
                )}

                {rangeProgress && rangeProgress.summary && (
                    <div className="progress-summary">
                        <h4>Download Summary</h4>
                        <div className="progress-stats">
                            <div className="stat success">
                                <span className="stat-label">Cached</span>
                                <span className="stat-value">{rangeProgress.summary.cached}</span>
                            </div>
                            <div className="stat info">
                                <span className="stat-label">Fetched</span>
                                <span className="stat-value">{rangeProgress.summary.fetched}</span>
                            </div>
                            <div className="stat failed">
                                <span className="stat-label">Failed</span>
                                <span className="stat-value">{rangeProgress.summary.failed}</span>
                            </div>
                            <div className="stat total">
                                <span className="stat-label">Total</span>
                                <span className="stat-value">{rangeProgress.summary.total_requested}</span>
                            </div>
                        </div>

                        {rangeProgress.summary.fetched === 0 && rangeProgress.summary.failed === 0 && (
                            <div className="pill pill-success">All days served from cache</div>
                        )}

                        {rangeProgress.entries && rangeProgress.entries.length > 0 && (
                            <div className="files-list">
                                <h5>Per-day status:</h5>
                                <ul>
                                    {rangeProgress.entries.map((entry, idx) => (
                                        <li key={idx} className={`range-status-item status-${entry.status}`}>
                                            <span className="date">{entry.date}</span>
                                            <span className="type">{entry.type.toUpperCase()}</span>
                                            <span className="status">{entry.status}</span>
                                            {entry.records !== undefined && <span className="records">{entry.records} records</span>}
                                        </li>
                                    ))}
                                </ul>
                            </div>
                        )}

                        {rangeProgress.errors && rangeProgress.errors.length > 0 && (
                            <div className="errors-list">
                                <h5>Failed Downloads:</h5>
                                <ul>
                                    {rangeProgress.errors.map((err, idx) => (
                                        <li key={idx} className="error-item">
                                            ❌ {err.date} - {err.error}
                                        </li>
                                    ))}
                                </ul>
                            </div>
                        )}
                    </div>
                )}

                <div className="download-info">
                    <h4>How it works:</h4>
                    <ol>
                        <li>Select start date (e.g., 01-Dec-2025)</li>
                        <li>Select end date (e.g., 05-Dec-2025)</li>
                        <li>Click "📅 Download Date Range"</li>
                        <li>All trading days between dates are downloaded automatically</li>
                        <li>Files saved with pattern: mcapDDMMYYYY.csv</li>
                        <li>Go to "Upload & Process" to consolidate all downloaded files</li>
                    </ol>
                </div>
            </div>
        </section>
    );
};

export default RangeTab;
