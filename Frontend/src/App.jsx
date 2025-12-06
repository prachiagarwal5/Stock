import React, { useState } from 'react';
import './App.css';

// Get API URL from environment variables or use default
const API_URL = import.meta.env.VITE_API_URL;

function App() {
    const [uploadedFiles, setUploadedFiles] = useState([]);
    const [loading, setLoading] = useState(false);
    const [preview, setPreview] = useState(null);
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(null);
    const [activeTab, setActiveTab] = useState('upload');

    const handleFileChange = (e) => {
        const files = Array.from(e.target.files);
        setUploadedFiles(files);
        setError(null);
        setSuccess(null);
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

        try {
            const response = await fetch(`${API_URL}/api/preview`, {
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

        try {
            const response = await fetch(`${API_URL}/api/consolidate`, {
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
                        className={`tab-btn ${activeTab === 'upload' ? 'active' : ''}`}
                        onClick={() => setActiveTab('upload')}
                    >
                        📤 Upload & Process
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
                )}
            </main>

            <footer className="footer">
                <p>💼 Market Cap Consolidation Tool | Powered by React & Flask</p>
            </footer>
        </div>
    );
}

export default App;
