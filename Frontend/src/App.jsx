import React, { useState } from 'react';
import './App.css';

function App() {
    const [uploadedFiles, setUploadedFiles] = useState([]);
    const [loading, setLoading] = useState(false);
    const [preview, setPreview] = useState(null);
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(null);
    const [activeTab, setActiveTab] = useState('upload');
    const [nseDate, setNseDate] = useState('');
    const [availableDates, setAvailableDates] = useState([]);
    const [nseLoading, setNseLoading] = useState(false);
    const [rangeStartDate, setRangeStartDate] = useState('');
    const [rangeEndDate, setRangeEndDate] = useState('');
    const [rangeLoading, setRangeLoading] = useState(false);
    const [rangeProgress, setRangeProgress] = useState(null);
    const [downloadDestination, setDownloadDestination] = useState('local');
    const [googleDriveStatus, setGoogleDriveStatus] = useState(null);
    const [googleDriveFiles, setGoogleDriveFiles] = useState([]);
    // NEW: Scrape session states
    const [scrapeSession, setScrapeSession] = useState(null);
    const [scrapeSessionLoading, setScrapeSessionLoading] = useState(false);
    const [scrapeSessionPreview, setScrapeSessionPreview] = useState(null);
    const [scrapeDownloadDestination, setScrapeDownloadDestination] = useState('local');
    const [dashboardResult, setDashboardResult] = useState(null);
    const [dashboardLoading, setDashboardLoading] = useState(false);
    const [dashboardError, setDashboardError] = useState(null);
    const [dashboardLimit, setDashboardLimit] = useState(100);
    const [dashboardPage, setDashboardPage] = useState(1);
    const [dashboardPageSize, setDashboardPageSize] = useState(150);

    // Fetch available NSE dates and Google Drive status on component mount
    React.useEffect(() => {
        fetchAvailableDates();
        checkGoogleDriveStatus();
    }, []);

    // Load dashboard data when tab is active
    React.useEffect(() => {
        if (activeTab === 'dashboard') {
            loadDashboardData(dashboardLimit);
        }
    }, [activeTab, dashboardLimit]);

    const fetchAvailableDates = async () => {
        try {
            const response = await fetch('http://localhost:5000/api/nse-dates');
            if (response.ok) {
                const data = await response.json();
                setAvailableDates(data.dates);
                if (data.dates.length > 0) {
                    // Convert DD-Mon-YYYY to YYYY-MM-DD for date input
                    const dateStr = data.dates[0];
                    const date = new Date(dateStr + ' 2025');
                    const formattedDate = date.toISOString().split('T')[0];
                    setNseDate(formattedDate);
                }
            }
        } catch (err) {
            console.error('Error fetching NSE dates:', err);
        }
    };

    const checkGoogleDriveStatus = async () => {
        try {
            const response = await fetch('http://localhost:5000/api/google-drive-status');
            if (response.ok) {
                const data = await response.json();
                setGoogleDriveStatus(data);
                if (data.authenticated) {
                    fetchGoogleDriveFiles();
                }
            }
        } catch (err) {
            console.error('Error checking Google Drive status:', err);
        }
    };

    const fetchGoogleDriveFiles = async () => {
        try {
            const response = await fetch('http://localhost:5000/api/google-drive-files');
            if (response.ok) {
                const data = await response.json();
                setGoogleDriveFiles(data.files || []);
            }
        } catch (err) {
            console.error('Error fetching Google Drive files:', err);
        }
    };

    // Convert YYYY-MM-DD (from date input) to DD-Mon-YYYY format for API
    const convertDateFormat = (dateStr) => {
        if (!dateStr) return '';
        const [year, month, day] = dateStr.split('-');
        const date = new Date(year, parseInt(month) - 1, parseInt(day));
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        return `${day}-${months[date.getMonth()]}-${year}`;
    };

    const handleDownloadFromNSE = async () => {
        if (!nseDate) {
            setError('Please select a date');
            return;
        }

        setNseLoading(true);
        setError(null);

        try {
            const formattedDate = convertDateFormat(nseDate);
            const response = await fetch('http://localhost:5000/api/download-nse', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    date: formattedDate,
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
                    start_date: convertDateFormat(rangeStartDate),
                    end_date: convertDateFormat(rangeEndDate),
                    save_to_file: true,
                    refresh_mode: 'missing_only'
                })
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Download failed');
            }

            const data = await response.json();
            setRangeProgress({
                summary: data.summary,
                entries: data.entries || [],
                errors: data.errors || []
            });

            const cached = data.summary.cached;
            const fetched = data.summary.fetched;
            const total = data.summary.total_requested;
            const message = fetched === 0 && data.summary.failed === 0
                ? `✅ All ${total} days served from cache`
                : `✅ Ready: cached ${cached}, fetched ${fetched}, total ${total}`;
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
        formData.append('download_destination', downloadDestination);

        try {
            const response = await fetch('http://localhost:5000/api/consolidate', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Download failed');
            }

            if (downloadDestination === 'google_drive') {
                // Google Drive upload - response is JSON
                const data = await response.json();
                setSuccess(`✅ File uploaded to Google Drive!\n📎 ${data.file_name}\n🔗 ${data.web_link}`);
                fetchGoogleDriveFiles(); // Refresh file list
            } else {
                // Local download - response is binary file
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
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    // NEW: Handlers for scrape session (preview/download options)
    const handleNSESingleDownloadSession = async () => {
        if (!nseDate) {
            setError('Please select a date');
            return;
        }

        setScrapeSessionLoading(true);
        setError(null);

        try {
            const formattedDate = convertDateFormat(nseDate);
            const response = await fetch('http://localhost:5000/api/download-nse', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    date: formattedDate,
                    save_to_file: false  // Temp storage mode
                })
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Download failed');
            }

            const data = await response.json();
            setScrapeSession({
                session_id: data.session_id,
                file: data.file,
                date: data.date,
                records_count: data.records_count,
                type: 'single'
            });
            setSuccess(`✅ Downloaded: ${data.file} (${data.records_count} records)`);
        } catch (err) {
            setError(err.message);
        } finally {
            setScrapeSessionLoading(false);
        }
    };

    const handleNSERangeDownloadSession = async () => {
        if (!rangeStartDate || !rangeEndDate) {
            setError('Please select both start and end dates');
            return;
        }

        if (rangeStartDate > rangeEndDate) {
            setError('Start date cannot be after end date');
            return;
        }

        setScrapeSessionLoading(true);
        setError(null);

        try {
            const response = await fetch('http://localhost:5000/api/download-nse-range', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    start_date: convertDateFormat(rangeStartDate),
                    end_date: convertDateFormat(rangeEndDate),
                    save_to_file: false,  // Temp storage mode
                    refresh_mode: 'missing_only'
                })
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Download failed');
            }

            const data = await response.json();
            setScrapeSession({
                session_id: data.session_id,
                type: 'range',
                summary: data.summary,
                entries: data.entries || [],
                errors: data.errors || []
            });
            setRangeProgress({
                summary: data.summary,
                entries: data.entries || [],
                errors: data.errors || []
            });
            const cached = data.summary.cached;
            const fetched = data.summary.fetched;
            const total = data.summary.total_requested;
            const message = fetched === 0 && data.summary.failed === 0
                ? `✅ All ${total} days served from cache`
                : `✅ Ready: cached ${cached}, fetched ${fetched}, total ${total}`;
            setSuccess(message);
        } catch (err) {
            setError(err.message);
        } finally {
            setScrapeSessionLoading(false);
        }
    };

    const loadDashboardData = async (limit) => {
        setDashboardLoading(true);
        setDashboardError(null);
        try {
            const response = await fetch(`http://localhost:5000/api/dashboard-data?limit=${limit}`);
            if (!response.ok) {
                const err = await response.json();
                throw new Error(err.error || 'Failed to load dashboard data');
            }
            const data = await response.json();
            setDashboardResult(data);
        } catch (err) {
            setDashboardError(err.message);
            setDashboardResult(null);
        } finally {
            setDashboardLoading(false);
        }
    };

    const handlePreviewScrapeSession = async () => {
        if (!scrapeSession) {
            setError('No scrape session active');
            return;
        }

        setScrapeSessionLoading(true);
        setError(null);

        try {
            const response = await fetch(`http://localhost:5000/api/scrape-session/${scrapeSession.session_id}/preview`);

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Preview failed');
            }

            const data = await response.json();
            setScrapeSessionPreview(data);
            setSuccess('✅ Preview loaded');
        } catch (err) {
            setError(err.message);
        } finally {
            setScrapeSessionLoading(false);
        }
    };

    const handleDownloadSingleCSV = async (filename) => {
        if (!scrapeSession) {
            setError('No scrape session active');
            return;
        }

        try {
            const response = await fetch(
                `http://localhost:5000/api/scrape-session/${scrapeSession.session_id}/download-csv?filename=${encodeURIComponent(filename)}`
            );

            if (!response.ok) {
                throw new Error('Download failed');
            }

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = filename;
            document.body.appendChild(link);
            link.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(link);

            setSuccess(`✅ Downloaded: ${filename}`);
        } catch (err) {
            setError(err.message);
        }
    };

    const handleConsolidateScrapeSession = async () => {
        if (!scrapeSession) {
            setError('No scrape session active');
            return;
        }

        setScrapeSessionLoading(true);
        setError(null);

        try {
            const response = await fetch(
                `http://localhost:5000/api/scrape-session/${scrapeSession.session_id}/consolidate`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        download_destination: scrapeDownloadDestination,
                        file_type: 'both'  // Request both mcap and pr files
                    })
                }
            );

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Consolidation failed');
            }

            if (scrapeDownloadDestination === 'google_drive') {
                const data = await response.json();
                let successMsg = '✅ Files uploaded to Google Drive!\n';

                // Show both file types if available
                if (data.downloads && Array.isArray(data.downloads)) {
                    data.downloads.forEach(file => {
                        const fileType = file.type === 'mcap' ? '📊 Market Cap' : '📈 Net Traded Value';
                        successMsg += `\n${fileType}: ${file.file_name}\n🔗 ${file.web_link}`;
                    });
                } else {
                    successMsg += `📎 ${data.file_name}\n🔗 ${data.web_link}`;
                }

                setSuccess(successMsg);
                fetchGoogleDriveFiles();
            } else {
                const blob = await response.blob();
                const url = window.URL.createObjectURL(blob);
                const link = document.createElement('a');
                link.href = url;

                // Detect if it's a zip or single Excel file
                const contentType = response.headers.get('content-type');
                const isZip = contentType && contentType.includes('zip');
                link.download = isZip ? 'Market_Data.zip' : 'Market_Data.xlsx';

                document.body.appendChild(link);
                link.click();
                window.URL.revokeObjectURL(url);
                document.body.removeChild(link);

                setSuccess(isZip ?
                    '✅ Excel files downloaded successfully (zipped)!' :
                    '✅ Excel file downloaded successfully!'
                );
            }

            // Clear session after successful export
            setScrapeSession(null);
            setScrapeSessionPreview(null);
        } catch (err) {
            setError(err.message);
        } finally {
            setScrapeSessionLoading(false);
        }
    };

    const handleCleanupScrapeSession = async () => {
        if (!scrapeSession) {
            setError('No scrape session active');
            return;
        }

        try {
            const response = await fetch(
                `http://localhost:5000/api/scrape-session/${scrapeSession.session_id}/cleanup`,
                {
                    method: 'POST'
                }
            );

            if (response.ok) {
                setScrapeSession(null);
                setScrapeSessionPreview(null);
                setSuccess('✅ Session cleaned up');
            }
        } catch (err) {
            setError(err.message);
        }
    };

    const formatNumber = (value) => {
        if (value === null || value === undefined || Number.isNaN(value)) return 'N/A';
        if (Math.abs(value) >= 1e6) {
            return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
        }
        return Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 });
    };

    const handleBuildDashboard = async () => {
        const topNInput = window.prompt('Enter top N symbols by MCAP average (leave blank for all symbols):', '1000');
        if (topNInput === null) {
            return; // user cancelled
        }

        const payload = {
            save_to_file: true,
            page: dashboardPage,
            page_size: dashboardPageSize
        };

        const topNVal = parseInt(topNInput, 10);
        if (!Number.isNaN(topNVal) && topNVal > 0) {
            payload.top_n = topNVal;
            payload.top_n_by = 'mcap';
            payload.parallel_workers = 10;
            payload.chunk_size = 100;
        }

        if (scrapeSession && scrapeSession.session_id) {
            payload.session_id = scrapeSession.session_id;
        } else if (nseDate) {
            payload.date = convertDateFormat(nseDate);
        } else {
            setError('No session or date selected for dashboard');
            return;
        }

        setDashboardLoading(true);
        setError(null);
        setSuccess(null);

        try {
            const response = await fetch('http://localhost:5000/api/nse-symbol-dashboard', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                const errorData = await response.json();
                console.error('[dashboard] failed status', response.status, errorData);
                throw new Error(errorData.error || 'Dashboard build failed');
            }

            const data = await response.json();
            console.log('[dashboard] success', data);
            setDashboardResult(data);
            const start = data.range?.start || 1;
            const end = data.range?.end || data.symbols_used || data.count || 0;
            setSuccess(`✅ Dashboard ready for symbols ${start}-${end} of ${data.total_symbols || data.symbols_used || data.count || 0}`);
        } catch (err) {
            console.error('[dashboard] error', err);
            setError(err.message);
        } finally {
            setDashboardLoading(false);
        }
    };

    const handleDownloadDashboard = async () => {
        if (!dashboardResult || !dashboardResult.download_url) {
            setError('No dashboard available to download');
            return;
        }

        try {
            const response = await fetch(`http://localhost:5000${dashboardResult.download_url}`);
            if (!response.ok) {
                throw new Error('Dashboard download failed');
            }

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = dashboardResult.file || 'Symbol_Dashboard.xlsx';
            document.body.appendChild(link);
            link.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(link);
            setSuccess('✅ Dashboard downloaded');
        } catch (err) {
            setError(err.message);
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
                        className={`tab-btn ${activeTab === 'preview' ? 'active' : ''}`}
                        onClick={() => setActiveTab('preview')}
                    >
                        👁️ Preview
                    </button>
                    <button
                        className={`tab-btn ${activeTab === 'mongo' ? 'active' : ''}`}
                        onClick={() => setActiveTab('mongo')}
                    >
                        🗄️ Mongo Dashboard
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

                            <div className="form-group">
                                <p className="info-box">
                                    <span className="info-icon">ℹ️</span>
                                    This will download the CM - Bhavcopy (PR.zip) from NSE and extract the CSV file.
                                    The file will be saved as <strong>mcapDDMMYYYY.csv</strong> in the Backend/nosubject/ folder.
                                </p>
                            </div>

                            <button
                                className="btn btn-secondary btn-large"
                                onClick={handleNSESingleDownloadSession}
                                disabled={scrapeSessionLoading || !nseDate}
                                title="Download and preview before consolidation - no permanent file storage"
                            >
                                {scrapeSessionLoading ? '⏳ Downloading...' : '👁️ Preview & Process'}
                            </button>

                            {scrapeSession && scrapeSession.type === 'single' && (
                                <div className="scrape-session-panel">
                                    <h3>📋 Downloaded Data - Ready to Process</h3>
                                    <div className="session-info">
                                        <p><strong>File:</strong> {scrapeSession.file}</p>
                                        <p><strong>Date:</strong> {scrapeSession.date}</p>
                                        <p><strong>Records:</strong> {scrapeSession.records_count}</p>
                                    </div>
                                    <div className="session-actions">
                                        <button
                                            className="btn btn-info"
                                            onClick={handlePreviewScrapeSession}
                                            disabled={scrapeSessionLoading}
                                        >
                                            👁️ Preview Data
                                        </button>
                                        <button
                                            className="btn btn-outline"
                                            onClick={handleBuildDashboard}
                                            disabled={scrapeSessionLoading || dashboardLoading}
                                        >
                                            {dashboardLoading ? '⏳ Building...' : '📊 Build Dashboard'}
                                        </button>
                                        <button
                                            className="btn btn-warning"
                                            onClick={() => handleDownloadSingleCSV(scrapeSession.file)}
                                            disabled={scrapeSessionLoading}
                                        >
                                            📥 Download CSV
                                        </button>
                                        <button
                                            className="btn btn-success"
                                            onClick={handleConsolidateScrapeSession}
                                            disabled={scrapeSessionLoading}
                                        >
                                            ✅ Export to Excel
                                        </button>
                                        <button
                                            className="btn btn-danger"
                                            onClick={handleCleanupScrapeSession}
                                        >
                                            🗑️ Cancel
                                        </button>
                                    </div>
                                </div>
                            )}

                            {scrapeSessionPreview && (
                                <div className="scrape-preview-panel">
                                    <h3>📊 Data Preview</h3>
                                    {scrapeSessionPreview.previews.map((preview, idx) => (
                                        <div key={idx} className="file-preview">
                                            <h4>{preview.filename}</h4>
                                            <p>{preview.total_records} total records</p>
                                            <table>
                                                <thead>
                                                    <tr>
                                                        {preview.columns.map((col, i) => (
                                                            <th key={i}>{col}</th>
                                                        ))}
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {preview.preview.map((row, rowIdx) => (
                                                        <tr key={rowIdx}>
                                                            {row.map((cell, colIdx) => (
                                                                <td key={colIdx}>{cell}</td>
                                                            ))}
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    ))}
                                </div>
                            )}

                            {dashboardResult && (
                                <div className="dashboard-panel">
                                    <div className="dashboard-header">
                                        <div>
                                            <h3>📊 Symbol Dashboard</h3>
                                            <p className="dashboard-sub">Page {dashboardResult.page} of {dashboardResult.total_pages} • Showing {dashboardResult.range?.start}–{dashboardResult.range?.end} of {dashboardResult.total_symbols || dashboardResult.symbols_used || 0}</p>
                                        </div>
                                        <div className="pill pill-info">{dashboardResult.symbols_used || dashboardResult.count || 0} symbols</div>
                                    </div>

                                    <div className="dashboard-controls">
                                        <label>
                                            Page
                                            <input
                                                type="number"
                                                min="1"
                                                value={dashboardPage}
                                                onChange={(e) => setDashboardPage(Number(e.target.value) || 1)}
                                            />
                                        </label>
                                        <label>
                                            Page Size
                                            <input
                                                type="number"
                                                min="10"
                                                max="300"
                                                value={dashboardPageSize}
                                                onChange={(e) => setDashboardPageSize(Number(e.target.value) || 150)}
                                            />
                                        </label>
                                        <button
                                            className="btn btn-outline"
                                            onClick={handleBuildDashboard}
                                            disabled={dashboardLoading}
                                        >
                                            {dashboardLoading ? '⏳ Refreshing...' : '🔄 Refresh'}
                                        </button>
                                    </div>

                                    <div className="dashboard-grid">
                                        <div className="stat">
                                            <span className="stat-label">Impact Cost (avg)</span>
                                            <span className="stat-value">{formatNumber(dashboardResult.averages?.impact_cost)}</span>
                                        </div>
                                        <div className="stat">
                                            <span className="stat-label">Free Float Mcap (avg)</span>
                                            <span className="stat-value">{formatNumber(dashboardResult.averages?.free_float_mcap)}</span>
                                        </div>
                                        <div className="stat">
                                            <span className="stat-label">Total Mcap (avg)</span>
                                            <span className="stat-value">{formatNumber(dashboardResult.averages?.total_market_cap)}</span>
                                        </div>
                                        <div className="stat">
                                            <span className="stat-label">Traded Value (avg)</span>
                                            <span className="stat-value">{formatNumber(dashboardResult.averages?.total_traded_value)}</span>
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
                                    {dashboardResult.errors && dashboardResult.errors.length > 0 && (
                                        <div className="dashboard-errors">
                                            <h5>Failed symbols</h5>
                                            <ul>
                                                {dashboardResult.errors.slice(0, 10).map((err, idx) => (
                                                    <li key={idx}>
                                                        {err.symbol}: {err.error}
                                                    </li>
                                                ))}
                                                {dashboardResult.errors.length > 10 && (
                                                    <li>...and {dashboardResult.errors.length - 10} more</li>
                                                )}
                                            </ul>
                                        </div>
                                    )}
                                </div>
                            )}

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
                                className="btn btn-secondary btn-large"
                                onClick={handleNSERangeDownloadSession}
                                disabled={scrapeSessionLoading || !rangeStartDate || !rangeEndDate}
                                title="Download and preview before consolidation - no permanent file storage"
                            >
                                {scrapeSessionLoading ? '⏳ Downloading...' : '👁️ Preview & Process'}
                            </button>

                            {scrapeSession && scrapeSession.type === 'range' && (
                                <div className="scrape-session-panel">
                                    <h3>📋 Downloaded Data Range - Ready to Process</h3>
                                    <div className="session-info">
                                        <p><strong>Cached:</strong> {scrapeSession.summary.cached}</p>
                                        <p><strong>Fetched:</strong> {scrapeSession.summary.fetched}</p>
                                        <p><strong>Failed:</strong> {scrapeSession.summary.failed}</p>
                                        <p><strong>Total Requested:</strong> {scrapeSession.summary.total_requested}</p>
                                        {scrapeSession.summary.fetched === 0 && scrapeSession.summary.failed === 0 && (
                                            <div className="pill pill-success">All days served from cache</div>
                                        )}
                                    </div>
                                    {scrapeSession.entries && scrapeSession.entries.length > 0 && (
                                        <div className="range-status-list">
                                            <div className="range-status-header">Per-day status</div>
                                            <ul>
                                                {scrapeSession.entries.map((entry, idx) => (
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
                                    <div className="session-actions">
                                        <button
                                            className="btn btn-info"
                                            onClick={handlePreviewScrapeSession}
                                            disabled={scrapeSessionLoading}
                                        >
                                            👁️ Preview All Data
                                        </button>
                                        <button
                                            className="btn btn-outline"
                                            onClick={handleBuildDashboard}
                                            disabled={scrapeSessionLoading || dashboardLoading}
                                        >
                                            {dashboardLoading ? '⏳ Building...' : '📊 Build Dashboard'}
                                        </button>
                                        <button
                                            className="btn btn-success"
                                            onClick={handleConsolidateScrapeSession}
                                            disabled={scrapeSessionLoading}
                                        >
                                            ✅ Export to Excel
                                        </button>
                                        <button
                                            className="btn btn-danger"
                                            onClick={handleCleanupScrapeSession}
                                        >
                                            🗑️ Cancel
                                        </button>
                                    </div>
                                </div>
                            )}

                            {scrapeSessionPreview && scrapeSession.type === 'range' && (
                                <div className="scrape-preview-panel">
                                    <h3>📊 Data Preview - Range</h3>
                                    {scrapeSessionPreview.previews.map((preview, idx) => (
                                        <div key={idx} className="file-preview">
                                            <h4>{preview.filename}</h4>
                                            <p>{preview.total_records} total records</p>
                                            <table style={{ fontSize: '0.8em' }}>
                                                <thead>
                                                    <tr>
                                                        {preview.columns.slice(0, 5).map((col, i) => (
                                                            <th key={i}>{col}</th>
                                                        ))}
                                                        {preview.columns.length > 5 && <th>...</th>}
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {preview.preview.slice(0, 5).map((row, rowIdx) => (
                                                        <tr key={rowIdx}>
                                                            {row.slice(0, 5).map((cell, colIdx) => (
                                                                <td key={colIdx}>{typeof cell === 'number' ? cell.toLocaleString() : cell}</td>
                                                            ))}
                                                            {row.length > 5 && <td>...</td>}
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    ))}
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

                        <div className="download-options">
                            <h3>📥 Download Destination</h3>
                            <div className="destination-buttons">
                                <button
                                    className={`destination-btn ${downloadDestination === 'local' ? 'active' : ''}`}
                                    onClick={() => setDownloadDestination('local')}
                                >
                                    💻 Download Locally
                                </button>
                                <button
                                    className={`destination-btn ${downloadDestination === 'google_drive' ? 'active' : ''} ${!googleDriveStatus?.authenticated ? 'disabled' : ''}`}
                                    onClick={() => {
                                        if (googleDriveStatus?.authenticated) {
                                            setDownloadDestination('google_drive');
                                        } else {
                                            setError('Google Drive is not configured. Please add credentials.json');
                                        }
                                    }}
                                    disabled={!googleDriveStatus?.authenticated}
                                    title={googleDriveStatus?.authenticated ? 'Save to Google Drive' : 'Google Drive not configured'}
                                >
                                    ☁️ Save to Google Drive
                                </button>
                            </div>
                            {googleDriveStatus?.authenticated && (
                                <div className="google-drive-info">
                                    <p>✅ <strong>Google Drive Connected</strong></p>
                                    <p>Files will be saved in the <strong>Automation</strong> folder</p>
                                    {googleDriveFiles.length > 0 && (
                                        <details>
                                            <summary>📁 View saved files ({googleDriveFiles.length})</summary>
                                            <ul className="drive-files-list">
                                                {googleDriveFiles.map((file, idx) => (
                                                    <li key={idx}>
                                                        📄 {file.name}
                                                        <a href={file.webViewLink} target="_blank" rel="noopener noreferrer" className="drive-link">
                                                            View
                                                        </a>
                                                    </li>
                                                ))}
                                            </ul>
                                        </details>
                                    )}
                                </div>
                            )}
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
                                {loading ? '⏳ Processing...' : downloadDestination === 'local' ? '⬇️ Download Excel' : '☁️ Upload to Drive'}
                            </button>
                        </div>
                    </section>
                )}

                {activeTab === 'mongo' && (
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
                                                        <td>{Array.isArray(row.indexList) ? row.indexList.slice(0, 2).join(', ') : (row.index || '')}</td>
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
