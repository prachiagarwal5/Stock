import React from 'react';

const formatNumber = (value) => {
    if (value === null || value === undefined || Number.isNaN(value)) return 'N/A';
    if (Math.abs(value) >= 1e6) {
        return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    return Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 });
};

const MongoTab = ({
    dashboardLimit,
    setDashboardLimit,
    dashboardLoading,
    dashboardError,
    dashboardResult,
    indicesLoading,
    loadDashboardData,
    handleUpdateIndices
}) => {
    return (
        <section className="section">
            <div className="section-header">
                <div>
                    <h2>🗄️ Mongo Data Dashboard</h2>
                    <p className="section-hint">Live view of stored aggregates and symbol metrics from MongoDB</p>
                </div>
                <div className="mongo-actions">
                    <label className="form-inline">
                        Limit
                        <input
                            type="number"
                            min="10"
                            max="500"
                            value={dashboardLimit}
                            onChange={(e) => setDashboardLimit(Number(e.target.value) || 100)}
                        />
                    </label>
                    <button className="btn btn-outline" onClick={() => loadDashboardData(dashboardLimit)} disabled={dashboardLoading}>
                        {dashboardLoading ? '⏳ Loading...' : '🔄 Refresh'}
                    </button>
                    <button
                        className="btn btn-primary"
                        onClick={handleUpdateIndices}
                        disabled={indicesLoading}
                    >
                        {indicesLoading ? '⏳ Updating...' : '🧭 Update Indices'}
                    </button>
                </div>
            </div>

            {dashboardError && <div className="alert alert-error">{dashboardError}</div>}

            <div className="mongo-grid">
                <div className="mongo-card">
                    <div className="mongo-card-head">
                        <h3>Market Cap Averages</h3>
                        <span className="pill pill-info">mcap</span>
                    </div>
                    <div className="table-wrap">
                        <table className="mongo-table">
                            <thead>
                                <tr>
                                    <th>Symbol</th>
                                    <th>Company</th>
                                    <th>Days</th>
                                    <th>Average</th>
                                </tr>
                            </thead>
                            <tbody>
                                {dashboardResult?.aggregates?.mcap?.length ? (
                                    dashboardResult.aggregates.mcap.map((row, idx) => (
                                        <tr key={idx}>
                                            <td>{row.symbol}</td>
                                            <td>{row.company_name}</td>
                                            <td>{row.days_with_data}</td>
                                            <td>{formatNumber(row.average)}</td>
                                        </tr>
                                    ))
                                ) : (
                                    <tr><td colSpan="4" className="empty">No data</td></tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                </div>

                <div className="mongo-card">
                    <div className="mongo-card-head">
                        <h3>Net Traded Value Averages</h3>
                        <span className="pill pill-info">pr</span>
                    </div>
                    <div className="table-wrap">
                        <table className="mongo-table">
                            <thead>
                                <tr>
                                    <th>Symbol</th>
                                    <th>Company</th>
                                    <th>Days</th>
                                    <th>Average</th>
                                </tr>
                            </thead>
                            <tbody>
                                {dashboardResult?.aggregates?.pr?.length ? (
                                    dashboardResult.aggregates.pr.map((row, idx) => (
                                        <tr key={idx}>
                                            <td>{row.symbol}</td>
                                            <td>{row.company_name}</td>
                                            <td>{row.days_with_data}</td>
                                            <td>{formatNumber(row.average)}</td>
                                        </tr>
                                    ))
                                ) : (
                                    <tr><td colSpan="4" className="empty">No data</td></tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                </div>

                <div className="mongo-card wide">
                    <div className="mongo-card-head">
                        <h3>Symbol Metrics</h3>
                        <span className="pill pill-info">nse metrics</span>
                    </div>
                    <div className="table-wrap">
                        <table className="mongo-table">
                            <thead>
                                <tr>
                                    <th>Symbol</th>
                                    <th>Company</th>
                                    <th>Series</th>
                                    <th>Index</th>
                                    <th>Impact Cost</th>
                                    <th>FF Mcap</th>
                                    <th>Total Mcap</th>
                                    <th>Traded Value</th>
                                    <th>Last Price</th>
                                </tr>
                            </thead>
                            <tbody>
                                {dashboardResult?.metrics?.length ? (
                                    dashboardResult.metrics.map((row, idx) => (
                                        <tr key={idx}>
                                            <td>{row.symbol}</td>
                                            <td>{row.companyName || row.company_name}</td>
                                            <td>{row.series}</td>
                                            <td>{row.primary_index || (Array.isArray(row.indexList) ? row.indexList.slice(0, 2).join(', ') : (row.index || ''))}</td>
                                            <td>{formatNumber(row.impact_cost)}</td>
                                            <td>{formatNumber(row.free_float_mcap)}</td>
                                            <td>{formatNumber(row.total_market_cap)}</td>
                                            <td>{formatNumber(row.total_traded_value)}</td>
                                            <td>{formatNumber(row.last_price)}</td>
                                        </tr>
                                    ))
                                ) : (
                                    <tr><td colSpan="9" className="empty">No data</td></tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </section>
    );
};

export default MongoTab;
