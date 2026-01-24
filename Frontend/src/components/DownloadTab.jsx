import React from 'react';

const DownloadTab = ({
    nseDate,
    setNseDate,
    nseLoading,
    exportLoading,
    exportLog,
    handleDownloadFromNSE,
    handleExportConsolidated
}) => {


    return (
        <section className="section">
            <div className="section-header-clean">
                <h2>Download Market Cap Data from NSE</h2>
                <p className="section-subtitle">
                    Automatically download Bhavcopy data from NSE website and save as mcapDDMMYYYY.csv
                </p>
            </div>

            <div className="card">
                <div className="form-group">
                    <label className="form-label">Select Date</label>
                    <input
                        type="date"
                        value={nseDate}
                        onChange={(e) => setNseDate(e.target.value)}
                        className="form-input"
                        disabled={nseLoading}
                    />
                    <small className="form-hint">
                        Select any trading date from the last 2 years
                    </small>
                </div>

                <div className="info-banner">
                    <div className="info-banner-icon">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                            <circle cx="12" cy="12" r="10"></circle>
                            <line x1="12" y1="16" x2="12" y2="12"></line>
                            <line x1="12" y1="8" x2="12.01" y2="8"></line>
                        </svg>
                    </div>
                    <span>
                        Downloads CM - Bhavcopy (PR.zip) from NSE and extracts the CSV file.
                        Saved as <code>mcapDDMMYYYY.csv</code> in Backend/nosubject/ folder.
                    </span>
                </div>

                <div className="button-row">
                    <button
                        className="btn btn-primary"
                        onClick={handleDownloadFromNSE}
                        disabled={nseLoading || exportLoading || !nseDate}
                    >
                        {nseLoading || exportLoading ? (
                            <>
                                <span className="spinner"></span>
                                Executing...
                            </>
                        ) : (
                            <>
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                                    <polyline points="7 10 12 15 17 10"></polyline>
                                    <line x1="12" y1="15" x2="12" y2="3"></line>
                                </svg>
                                Execute All Processes
                            </>
                        )}
                    </button>
                </div>

            </div>

            <div className="card card-muted">
                <h4 className="card-title">How it works</h4>
                <ol className="steps-list">
                    <li>Select a date from the dropdown</li>
                    <li>Click "Execute All Processes" to download NSE data for the selected date</li>
                    <li>Data will be saved automatically to the backend</li>
                </ol>
            </div>
        </section>
    );
};

export default DownloadTab;
