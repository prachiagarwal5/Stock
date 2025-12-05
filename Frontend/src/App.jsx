import React, { useState } from 'react';
import './App.css';

function App() {
    const [uploadedFiles, setUploadedFiles] = useState([]);
    const [corporateActions, setCorporateActions] = useState({
        splits: [],
        name_changes: [],
        delistings: []
    });
    const [loading, setLoading] = useState(false);
    const [preview, setPreview] = useState(null);
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(null);
    const [newSplit, setNewSplit] = useState({
        old_symbol: '',
        new_symbols: [],
        split_date: ''
    });
    const [activeTab, setActiveTab] = useState('upload');
    const [nseDate, setNseDate] = useState('');
    const [availableDates, setAvailableDates] = useState([]);
    const [nseLoading, setNseLoading] = useState(false);
    const [rangeStartDate, setRangeStartDate] = useState('');
    const [rangeEndDate, setRangeEndDate] = useState('');
    const [rangeLoading, setRangeLoading] = useState(false);
    const [rangeProgress, setRangeProgress] = useState(null);

    // Fetch available NSE dates on component mount
    React.useEffect(() => {
        fetchAvailableDates();
    }, []);

    const fetchAvailableDates = async () => {
        try {
            const response = await fetch('http://localhost:5000/api/nse-dates');
            if (response.ok) {
                const data = await response.json();
                setAvailableDates(data.dates);
                if (data.dates.length > 0) {
                    setNseDate(data.dates[0]);
                }
            }
        } catch (err) {
            console.error('Error fetching NSE dates:', err);
        }
    };

    const handleDownloadFromNSE = async () => {
        if (!nseDate) {
            setError('Please select a date');
            return;
        }

        setNseLoading(true);
        setError(null);

        try {
            const response = await fetch('http://localhost:5000/api/download-nse', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    date: nseDate,
                    save_to_file: true
                })
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Download failed');
            }

            const data = await response.json();
            setSuccess(`✅ Downloaded: ${data.file} (${data.records_count} records)`);

            // Clear uploaded files and add the new one conceptually
            setTimeout(() => {
                setActiveTab('upload');
            }, 2000);
        } catch (err) {
            setError(err.message);
        } finally {
            setNseLoading(false);
        }
    };

    const handleDownloadRangeFromNSE = async () => {
        if (!rangeStartDate || !rangeEndDate) {
            setError('Please select both start and end dates');
            return;
        }

        if (rangeStartDate > rangeEndDate) {
            setError('Start date cannot be after end date');
            return;
        }

        setRangeLoading(true);
        setError(null);
        setSuccess(null);
        setRangeProgress(null);

        try {
            const response = await fetch('http://localhost:5000/api/download-nse-range', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    start_date: rangeStartDate,
                    end_date: rangeEndDate,
                    save_to_file: true
                })
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Download failed');
            }

            const data = await response.json();
            setRangeProgress({
                success: data.summary.successful,
                failed: data.summary.failed,
                total: data.summary.total_requested,
                files: data.files,
                errors: data.errors
            });

            const message = `✅ Downloaded ${data.summary.successful}/${data.summary.total_requested} files`;
            setSuccess(message);

            setTimeout(() => {
                setActiveTab('upload');
            }, 3000);
        } catch (err) {
            setError(err.message);
        } finally {
            setRangeLoading(false);
        }
    };

    const handleFileChange = (e) => {
        const files = Array.from(e.target.files);
        setUploadedFiles(files);
        setError(null);
        setSuccess(null);
    };

    const handleAddSplit = () => {
        if (newSplit.old_symbol && newSplit.new_symbols.length > 0 && newSplit.split_date) {
            setCorporateActions({
                ...corporateActions,
                splits: [...corporateActions.splits, newSplit]
            });
            setNewSplit({
                old_symbol: '',
                new_symbols: [],
                split_date: ''
            });
            setSuccess('Stock split added successfully');
        } else {
            setError('Please fill in all split details');
        }
    };

    const handleRemoveSplit = (index) => {
        setCorporateActions({
            ...corporateActions,
            splits: corporateActions.splits.filter((_, i) => i !== index)
        });
    };

    const handleAddNewSymbol = (e, currentSymbols) => {
        const value = e.target.value;
        if (value && !currentSymbols.includes(value)) {
            setNewSplit({
                ...newSplit,
                new_symbols: [...currentSymbols, value]
            });
            e.target.value = '';
        }
    };

    const handleRemoveSymbol = (index) => {
        setNewSplit({
            ...newSplit,
            new_symbols: newSplit.new_symbols.filter((_, i) => i !== index)
        });
    };

    const handlePreview = async () => {
        if (uploadedFiles.length === 0) {
            setError('Please upload at least one CSV file');
            return;
        }

        setLoading(true);
        setError(null);

        const formData = new FormData();
        uploadedFiles.forEach(file => formData.append('files', file));
        formData.append('corporate_actions', JSON.stringify(corporateActions));

        try {
            const response = await fetch('http://localhost:5000/api/preview', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Preview failed');
            }

            const data = await response.json();
            setPreview(data);
            setSuccess('Preview loaded successfully');
        } catch (err) {
            setError(err.message);
            setPreview(null);
        } finally {
            setLoading(false);
        }
    };

    const handleDownload = async () => {
        if (uploadedFiles.length === 0) {
            setError('Please upload at least one CSV file');
            return;
        }

        setLoading(true);
        setError(null);

        const formData = new FormData();
        uploadedFiles.forEach(file => formData.append('files', file));
        formData.append('corporate_actions', JSON.stringify(corporateActions));

        try {
            const response = await fetch('http://localhost:5000/api/consolidate', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Download failed');
            }

            // Create blob from response
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = 'Finished_Product.xlsx';
            document.body.appendChild(link);
            link.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(link);

            setSuccess('Excel file downloaded successfully!');
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="app">
            <header className="header">
                <h1>📊 Market Cap Consolidation Tool</h1>
                <p>Upload CSV files and consolidate market cap data into a professional Excel file</p>
            </header>

            <main className="main-content">
                <div className="tabs">
                    <button
                        className={`tab-btn ${activeTab === 'download' ? 'active' : ''}`}
                        onClick={() => setActiveTab('download')}
                    >
                        🔽 Download from NSE
                    </button>
                    <button
                        className={`tab-btn ${activeTab === 'range' ? 'active' : ''}`}
                        onClick={() => setActiveTab('range')}
                    >
                        📅 Date Range Download
                    </button>
                    <button
                        className={`tab-btn ${activeTab === 'upload' ? 'active' : ''}`}
                        onClick={() => setActiveTab('upload')}
                    >
                        📤 Upload & Process
                    </button>
                    <button
                        className={`tab-btn ${activeTab === 'actions' ? 'active' : ''}`}
                        onClick={() => setActiveTab('actions')}
                    >
                        ⚙️ Corporate Actions
                    </button>
                    <button
                        className={`tab-btn ${activeTab === 'preview' ? 'active' : ''}`}
                        onClick={() => setActiveTab('preview')}
                    >
                        👁️ Preview
                    </button>
                </div>

                {error && <div className="alert alert-error">{error}</div>}
                {success && <div className="alert alert-success">{success}</div>}

                {activeTab === 'download' && (
                    <section className="section">
                        <h2>🔽 Download Market Cap Data from NSE</h2>
                        <p className="section-hint">
                            Automatically download Bhavcopy data from NSE website and save as mcapDDMMYYYY.csv
                        </p>

                        <div className="nse-download-panel">
                            <div className="form-group">
                                <label>Select Date</label>
                                <select
                                    value={nseDate}
                                    onChange={(e) => setNseDate(e.target.value)}
                                    className="form-input"
                                    disabled={nseLoading}
                                >
                                    {availableDates.map((date, idx) => (
                                        <option key={idx} value={date}>
                                            {date}
                                        </option>
                                    ))}
                                </select>
                                <small className="form-hint">
                                    Showing available trading dates (last 30 days, excluding weekends)
                                </small>
                            </div>

                            <div className="form-group">
                                <p className="info-box">
                                    <span className="info-icon">ℹ️</span>
                                    This will download the CM - Bhavcopy (PR.zip) from NSE and extract the CSV file.
                                    The file will be saved as <strong>mcapDDMMYYYY.csv</strong> in the Backend/nosubject/ folder.
                                </p>
                            </div>

                            <button
                                className="btn btn-primary btn-large"
                                onClick={handleDownloadFromNSE}
                                disabled={nseLoading || !nseDate}
                            >
                                {nseLoading ? '⏳ Downloading from NSE...' : '🔽 Download & Save CSV'}
                            </button>

                            <div className="download-info">
                                <h4>How it works:</h4>
                                <ol>
                                    <li>Select a date from the dropdown</li>
                                    <li>Click "Download & Save CSV"</li>
                                    <li>The file is downloaded from NSE and automatically saved</li>
                                    <li>Go to "Upload & Process" tab to consolidate your data</li>
                                </ol>
                            </div>
                        </div>
                    </section>
                )}

                {activeTab === 'range' && (
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
                                className="btn btn-primary btn-large"
                                onClick={handleDownloadRangeFromNSE}
                                disabled={rangeLoading || !rangeStartDate || !rangeEndDate}
                            >
                                {rangeLoading ? '⏳ Downloading files...' : '📅 Download Date Range'}
                            </button>

                            {rangeProgress && (
                                <div className="progress-summary">
                                    <h4>Download Summary</h4>
                                    <div className="progress-stats">
                                        <div className="stat success">
                                            <span className="stat-label">Successful</span>
                                            <span className="stat-value">{rangeProgress.success}</span>
                                        </div>
                                        <div className="stat failed">
                                            <span className="stat-label">Failed</span>
                                            <span className="stat-value">{rangeProgress.failed}</span>
                                        </div>
                                        <div className="stat total">
                                            <span className="stat-label">Total</span>
                                            <span className="stat-value">{rangeProgress.total}</span>
                                        </div>
                                    </div>

                                    {rangeProgress.files.length > 0 && (
                                        <div className="files-list">
                                            <h5>Downloaded Files:</h5>
                                            <ul>
                                                {rangeProgress.files.map((file, idx) => (
                                                    <li key={idx}>
                                                        ✅ {file.filename} - {file.records} records ({file.date})
                                                    </li>
                                                ))}
                                            </ul>
                                        </div>
                                    )}

                                    {rangeProgress.errors.length > 0 && (
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
                )}

                {error && <div className="alert alert-error">{error}</div>}
                {success && <div className="alert alert-success">{success}</div>}

                {activeTab === 'upload' && (
                    <section className="section">
                        <h2>Step 1: Upload CSV Files</h2>
                        <div className="upload-area">
                            <input
                                type="file"
                                id="file-input"
                                multiple
                                accept=".csv"
                                onChange={handleFileChange}
                                className="file-input"
                            />
                            <label htmlFor="file-input" className="upload-label">
                                <span className="upload-icon">📁</span>
                                <span>Drag and drop CSV files or click to select</span>
                                <span className="upload-hint">Supported: .csv files | Format: mcapDDMMYYYY.csv</span>
                            </label>
                        </div>

                        {uploadedFiles.length > 0 && (
                            <div className="file-list">
                                <h3>Uploaded Files ({uploadedFiles.length})</h3>
                                <ul>
                                    {uploadedFiles.map((file, index) => (
                                        <li key={index}>
                                            <span className="file-icon">📄</span>
                                            <span className="file-name">{file.name}</span>
                                            <span className="file-size">({(file.size / 1024).toFixed(2)} KB)</span>
                                        </li>
                                    ))}
                                </ul>
                            </div>
                        )}

                        <div className="action-buttons">
                            <button
                                className="btn btn-primary"
                                onClick={handlePreview}
                                disabled={uploadedFiles.length === 0 || loading}
                            >
                                {loading ? '⏳ Processing...' : '👁️ Preview Data'}
                            </button>
                            <button
                                className="btn btn-success"
                                onClick={handleDownload}
                                disabled={uploadedFiles.length === 0 || loading}
                            >
                                {loading ? '⏳ Processing...' : '⬇️ Download Excel'}
                            </button>
                        </div>
                    </section>
                )}

                {activeTab === 'actions' && (
                    <section className="section">
                        <h2>Step 2: Configure Corporate Actions</h2>
                        <p className="section-hint">
                            Define stock splits, name changes, and delistings to ensure data accuracy
                        </p>

                        <div className="corporate-actions-panel">
                            <h3>Add Stock Split/Demerger</h3>
                            <div className="form-group">
                                <label>Old Symbol (e.g., TATAMOTOR)</label>
                                <input
                                    type="text"
                                    placeholder="Enter old company symbol"
                                    value={newSplit.old_symbol}
                                    onChange={(e) => setNewSplit({ ...newSplit, old_symbol: e.target.value.toUpperCase() })}
                                    className="form-input"
                                />
                            </div>

                            <div className="form-group">
                                <label>New Symbols</label>
                                <div className="new-symbols-input">
                                    <input
                                        type="text"
                                        placeholder="Enter new symbol and press enter (e.g., TMPV)"
                                        onKeyPress={(e) => {
                                            if (e.key === 'Enter') {
                                                handleAddNewSymbol(e, newSplit.new_symbols);
                                            }
                                        }}
                                        className="form-input"
                                    />
                                </div>
                                {newSplit.new_symbols.length > 0 && (
                                    <div className="symbol-tags">
                                        {newSplit.new_symbols.map((symbol, idx) => (
                                            <span key={idx} className="symbol-tag">
                                                {symbol}
                                                <button
                                                    type="button"
                                                    onClick={() => handleRemoveSymbol(idx)}
                                                    className="tag-remove"
                                                >
                                                    ×
                                                </button>
                                            </span>
                                        ))}
                                    </div>
                                )}
                            </div>

                            <div className="form-group">
                                <label>Split Date (DD-MM-YYYY)</label>
                                <input
                                    type="date"
                                    value={newSplit.split_date}
                                    onChange={(e) => {
                                        const date = new Date(e.target.value);
                                        const formatted = `${String(date.getDate()).padStart(2, '0')}-${String(date.getMonth() + 1).padStart(2, '0')}-${date.getFullYear()}`;
                                        setNewSplit({ ...newSplit, split_date: formatted });
                                    }}
                                    className="form-input"
                                />
                            </div>

                            <button
                                className="btn btn-primary"
                                onClick={handleAddSplit}
                            >
                                ➕ Add Split
                            </button>
                        </div>

                        {corporateActions.splits.length > 0 && (
                            <div className="actions-list">
                                <h3>Configured Splits</h3>
                                {corporateActions.splits.map((split, index) => (
                                    <div key={index} className="action-item">
                                        <div className="action-details">
                                            <span className="action-badge">Split</span>
                                            <strong>{split.old_symbol}</strong>
                                            <span className="arrow">→</span>
                                            <span className="new-symbols">{split.new_symbols.join(', ')}</span>
                                            <span className="action-date">on {split.split_date}</span>
                                        </div>
                                        <button
                                            className="btn btn-small btn-danger"
                                            onClick={() => handleRemoveSplit(index)}
                                        >
                                            🗑️ Remove
                                        </button>
                                    </div>
                                ))}
                            </div>
                        )}
                    </section>
                )}

                {activeTab === 'preview' && (
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
                                                            {typeof cell === 'number' ? cell.toLocaleString('en-IN', {
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
                )}
            </main>

            <footer className="footer">
                <p>💼 Market Cap Consolidation Tool | Powered by React & Flask</p>
            </footer>
        </div>
    );
}

export default App;
