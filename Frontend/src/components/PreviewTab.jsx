import React from 'react';

const PreviewTab = ({ preview }) => {
    return (
        <section className="section">
            <h2>Step 3: Preview Results</h2>
            {preview ? (
                <div className="preview-container">
                    <div className="summary-cards">
                        <div className="summary-card">
                            <span className="summary-icon">🏢</span>
                            <div>
                                <div className="summary-label">Total Companies</div>
                                <div className="summary-value">{preview.summary.total_companies}</div>
                            </div>
                        </div>
                        <div className="summary-card">
                            <span className="summary-icon">📅</span>
                            <div>
                                <div className="summary-label">Total Dates</div>
                                <div className="summary-value">{preview.summary.total_dates}</div>
                            </div>
                        </div>
                        <div className="summary-card">
                            <span className="summary-icon">📤</span>
                            <div>
                                <div className="summary-label">Uploaded Files</div>
                                <div className="summary-value">{preview.summary.uploaded_files}</div>
                            </div>
                        </div>
                    </div>

                    <div className="dates-list">
                        <h4>📊 Dates Included:</h4>
                        <div className="dates-tags">
                            {preview.summary.dates.map((date, idx) => (
                                <span key={idx} className="date-tag">{date}</span>
                            ))}
                        </div>
                    </div>

                    <div className="preview-table">
                        <h4>Sample Data (First 10 Companies)</h4>
                        <table>
                            <thead>
                                <tr>
                                    {preview.preview.columns.map((col, idx) => (
                                        <th key={idx}>{col}</th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {preview.preview.data.map((row, rowIdx) => (
                                    <tr key={rowIdx}>
                                        {row.map((cell, colIdx) => (
                                            <td key={colIdx}>
                                                {cell === null || cell === undefined ? '' :
                                                    typeof cell === 'number' ? cell.toLocaleString('en-IN', {
                                                        minimumFractionDigits: 2,
                                                        maximumFractionDigits: 2
                                                    }) : cell}
                                            </td>
                                        ))}
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>
            ) : (
                <div className="no-preview">
                    <span>👁️</span>
                    <p>No preview available yet. Upload files and click "Preview Data" to see results.</p>
                </div>
            )}
        </section>
    );
};

export default PreviewTab;
