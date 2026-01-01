import React from 'react';

const UploadTab = ({
    uploadedFiles,
    loading,
    handleFileChange,
    handlePreview,
    handleDownload
}) => {
    return (
        <section className="section">
            <h2>
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#10b981" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                    <polyline points="14 2 14 8 20 8"/>
                    <line x1="16" y1="13" x2="8" y2="13"/>
                    <line x1="16" y1="17" x2="8" y2="17"/>
                    <polyline points="10 9 9 9 8 9"/>
                </svg>
                Upload CSV Files
            </h2>
            <p className="section-hint">Select one or more CSV files containing market cap data</p>
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
                    <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#10b981" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                        <polyline points="17 8 12 3 7 8"/>
                        <line x1="12" y1="3" x2="12" y2="15"/>
                    </svg>
                    <span>Drop CSV files here or click to browse</span>
                    <span className="upload-hint">Supported format: mcapDDMMYYYY.csv</span>
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

            <div className="download-options">
                <h3>📥 Download Destination</h3>
                <div className="destination-buttons">
                    <button className="destination-btn active" disabled>
                        💻 Download Locally
                    </button>
                </div>
            </div>

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
    );
};

export default UploadTab;
