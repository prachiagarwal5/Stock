import React, { useState } from 'react';
import Header from './components/Header';
import FileUpload from './components/FileUpload';
import PreviewSection from './components/PreviewSection';
import Alert from './components/Alert';
import { API_URL, ENDPOINTS } from './utils/constants';
import './App.css';

function App() {
    const [uploadedFiles, setUploadedFiles] = useState([]);
    const [previewLoading, setPreviewLoading] = useState(false);
    const [downloadLoading, setDownloadLoading] = useState(false);
    const [preview, setPreview] = useState(null);
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(null);
    const [activeTab, setActiveTab] = useState('upload');

    const handleFileChange = (files) => {
        setUploadedFiles(files);
        setError(null);
        setSuccess(null);
    };

    const handlePreview = async () => {
        if (uploadedFiles.length === 0) {
            setError('Please upload at least one CSV file');
            return;
        }

        setPreviewLoading(true);
        setError(null);

        const formData = new FormData();
        uploadedFiles.forEach((file) => formData.append('files', file));

        try {
            const response = await fetch(`${API_URL}${ENDPOINTS.PREVIEW}`, {
                method: 'POST',
                body: formData,
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Preview failed');
            }

            const data = await response.json();
            setPreview(data);
            setSuccess('Preview loaded successfully! Switch to Preview tab to view results.');
            setActiveTab('preview');
        } catch (err) {
            setError(err.message);
            setPreview(null);
        } finally {
            setPreviewLoading(false);
        }
    };

    const handleDownload = async () => {
        if (uploadedFiles.length === 0) {
            setError('Please upload at least one CSV file');
            return;
        }

        setDownloadLoading(true);
        setError(null);

        const formData = new FormData();
        uploadedFiles.forEach((file) => formData.append('files', file));

        try {
            const response = await fetch(`${API_URL}${ENDPOINTS.CONSOLIDATE}`, {
                method: 'POST',
                body: formData,
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

            setSuccess('Excel file downloaded successfully! Check your downloads folder.');
        } catch (err) {
            setError(err.message);
        } finally {
            setDownloadLoading(false);
        }
    };

    return (
        <div className="app">
            <Header />

            <main className="main-content">
                {/* Tabs */}
                <div className="tabs">
                    <button
                        className={`tab-button ${activeTab === 'upload' ? 'active' : ''}`}
                        onClick={() => setActiveTab('upload')}
                    >
                        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                            <polyline points="17 8 12 3 7 8"></polyline>
                            <line x1="12" y1="3" x2="12" y2="15"></line>
                        </svg>
                        Upload & Process
                    </button>
                    <button
                        className={`tab-button ${activeTab === 'preview' ? 'active' : ''}`}
                        onClick={() => setActiveTab('preview')}
                    >
                        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                            <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
                            <circle cx="12" cy="12" r="3"></circle>
                        </svg>
                        Preview Results
                    </button>
                </div>

                {/* Alerts */}
                {error && <Alert type="error" message={error} onClose={() => setError(null)} />}
                {success && <Alert type="success" message={success} onClose={() => setSuccess(null)} />}

                {/* Tab Content */}
                {activeTab === 'upload' && (
                    <FileUpload
                        files={uploadedFiles}
                        onFileChange={handleFileChange}
                        onPreview={handlePreview}
                        onDownload={handleDownload}
                        previewLoading={previewLoading}
                        downloadLoading={downloadLoading}
                    />
                )}

                {activeTab === 'preview' && <PreviewSection preview={preview} />}
            </main>

            <footer className="footer">
                <div className="footer-content">
                    <p>
                        💼 Market Cap Consolidation Tool
                        <span className="footer-separator">|</span>
                        Powered by React & Flask
                    </p>
                    <p className="footer-copyright">© 2025 All rights reserved</p>
                </div>
            </footer>
        </div>
    );
}

export default App;
