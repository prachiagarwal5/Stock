import React from 'react';
import Button from './Button';
import './FileUpload.css';

const FileUpload = ({ files, onFileChange, onPreview, onDownload, previewLoading, downloadLoading }) => {
    const handleFileInputChange = (e) => {
        const selectedFiles = Array.from(e.target.files);
        onFileChange(selectedFiles);
    };

    const formatFileSize = (bytes) => {
        return (bytes / 1024).toFixed(2);
    };

    const isAnyLoading = previewLoading || downloadLoading;

    return (
        <section className="file-upload-section animate-slideInBottom">
            <div className="section-header">
                <h2>📤 Upload CSV Files</h2>
                <p className="section-subtitle">Select one or more CSV files containing market cap data</p>
            </div>

            <div className="upload-zone">
                <input
                    type="file"
                    id="file-input"
                    multiple
                    accept=".csv"
                    onChange={handleFileInputChange}
                    className="file-input-hidden"
                />
                <label htmlFor="file-input" className="upload-label">
                    <div className="upload-icon">
                        <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                            <polyline points="17 8 12 3 7 8"></polyline>
                            <line x1="12" y1="3" x2="12" y2="15"></line>
                        </svg>
                    </div>
                    <div className="upload-text">
                        <p className="upload-title">Drop CSV files here or click to browse</p>
                        <p className="upload-hint">Supported format: mcapDDMMYYYY.csv</p>
                    </div>
                </label>
            </div>

            {files.length > 0 && (
                <div className="file-list animate-slideInBottom">
                    <h3 className="file-list-title">
                        📁 Selected Files ({files.length})
                    </h3>
                    <ul className="file-items">
                        {files.map((file, index) => (
                            <li key={index} className="file-item">
                                <div className="file-icon">
                                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                                        <polyline points="14 2 14 8 20 8"></polyline>
                                    </svg>
                                </div>
                                <span className="file-name">{file.name}</span>
                                <span className="file-size">{formatFileSize(file.size)} KB</span>
                            </li>
                        ))}
                    </ul>
                </div>
            )}

            <div className="action-buttons">
                <Button
                    variant="primary"
                    onClick={onPreview}
                    disabled={files.length === 0 || isAnyLoading}
                    loading={previewLoading}

                >
                    Preview Data
                </Button>
                <Button
                    variant="success"
                    onClick={onDownload}
                    disabled={files.length === 0 || isAnyLoading}
                    loading={downloadLoading}
                >
                    Download Excel
                </Button>
            </div>
        </section>
    );
};

export default FileUpload;
