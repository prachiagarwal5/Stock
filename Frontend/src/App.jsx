import React, { useState, useEffect } from 'react';
import './App.css';
import {
    Header,
    Footer,
    TabBar,
    AlertMessages,
    DownloadTab,
    RangeTab
} from './components';

// API URL from environment variable with fallback to localhost
const VITE_API_URL = import.meta.env.VITE_API_URL;

function App() {
    const [uploadedFiles, setUploadedFiles] = useState([]);
    const [loading, setLoading] = useState(false);
    const [pipelineLoading, setPipelineLoading] = useState(false);
    const [pipelineStage, setPipelineStage] = useState('');
    const [preview, setPreview] = useState(null);
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(null);
    const [activeTab, setActiveTab] = useState('download');
    const [nseDate, setNseDate] = useState('');
    const [availableDates, setAvailableDates] = useState([]);
    const [nseLoading, setNseLoading] = useState(false);
    const [rangeStartDate, setRangeStartDate] = useState('');
    const [rangeEndDate, setRangeEndDate] = useState('');
    const [rangeLoading, setRangeLoading] = useState(false);
    const [rangeProgress, setRangeProgress] = useState(null);
    const [downloadDestination] = useState('local');
    const [exportLoading, setExportLoading] = useState(false);
    const [exportLog, setExportLog] = useState([]);
    const [consolidationReady, setConsolidationReady] = useState(false);
    const [consolidationStatus, setConsolidationStatus] = useState(null);
    const [exportedRange, setExportedRange] = useState(null);
    const [dashboardResult, setDashboardResult] = useState(null);
    const [dashboardLoading, setDashboardLoading] = useState(false);
    const [dashboardBatchProgress, setDashboardBatchProgress] = useState(null);
    // Reset consolidation status when date range changes
    useEffect(() => {
        if (exportedRange) {
            if (rangeStartDate !== exportedRange.start || rangeEndDate !== exportedRange.end) {
                setConsolidationReady(false);
                setConsolidationStatus(null);
                setExportedRange(null);
            }
        }
    }, [rangeStartDate, rangeEndDate, exportedRange]);

    // Fetch available NSE dates on component mount
    useEffect(() => {
        fetchAvailableDates();
    }, []);

    const fetchAvailableDates = async () => {
        try {
            const response = await fetch(`${VITE_API_URL}/api/nse-dates`);
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

    // Convert YYYY-MM-DD (from date input) to DD-Mon-YYYY format for API
    const convertDateFormat = (dateStr) => {
        if (!dateStr) return '';
        const [year, month, day] = dateStr.split('-');
        const date = new Date(year, parseInt(month) - 1, parseInt(day));
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        return `${day}-${months[date.getMonth()]}-${year}`;
    };

    // Ask the user where to save a file; fall back to default downloads if picker is unavailable
    const saveBlobToChosenLocation = async (blob, defaultName) => {
        let pickerUsed = false;
        if (window.showSaveFilePicker) {
            try {
                const handle = await window.showSaveFilePicker({
                    suggestedName: defaultName,
                    types: [
                        {
                            description: 'Excel or Zip',
                            accept: {
                                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
                                'application/zip': ['.zip']
                            }
                        }
                    ]
                });
                const writable = await handle.createWritable();
                await writable.write(blob);
                await writable.close();
                pickerUsed = true;
                return { filename: handle.name || defaultName, pickerUsed };
            } catch (err) {
                if (err?.name === 'AbortError') {
                    throw new Error('Download cancelled');
                }
                // SecurityError is expected when called after async operations (not a direct user gesture)
                // Silently fall through to browser download fallback
            }
        }

        // Browser lacks picker or picker failed (async context); use standard download
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = defaultName;
        document.body.appendChild(link);
        link.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(link);
        // Don't show message for fallback - it's the expected behavior on deployed sites
        return { filename: defaultName, pickerUsed };
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
            const response = await fetch(`${VITE_API_URL}/api/download-nse`, {
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
            const mcapInfo = data.files?.mcap;
            const prInfo = data.files?.pr;
            const parts = [];
            if (mcapInfo) parts.push(`MCAP ${mcapInfo.records || 0} rows`);
            if (prInfo) parts.push(`PR ${prInfo.records || 0} rows`);
            setSuccess(`✅ Saved ${parts.join(' & ')} for ${data.date}`);
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
            const response = await fetch(`${VITE_API_URL}/api/download-nse-range`, {
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
        } catch (err) {
            setError(err.message);
        } finally {
            setRangeLoading(false);
        }
    };

    const handleExportConsolidated = async (scope = 'date', skipDaily = true) => {
        const payload = { file_type: 'both', fast_mode: false, skip_daily: skipDaily }; // persist averages to DB
        setExportLog(['Starting export...']);

        if (scope === 'range') {
            if (!rangeStartDate || !rangeEndDate) {
                setError('Select a start and end date');
                setExportLog([]);
                return;
            }
            payload.start_date = convertDateFormat(rangeStartDate);
            payload.end_date = convertDateFormat(rangeEndDate);
        } else {
            if (!nseDate) {
                setError('Select a date to export');
                setExportLog([]);
                return;
            }
            payload.date = convertDateFormat(nseDate);
        }

        setExportLoading(true);
        setError(null);
        setSuccess(null);

        try {
            const response = await fetch(`${VITE_API_URL}/api/consolidate-saved`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            const headerLog = response.headers.get('x-export-log');
            if (headerLog) {
                setExportLog(headerLog.split('\n'));
            }

            const contentType = response.headers.get('content-type') || '';
            if (!response.ok && contentType.includes('application/json')) {
                const errData = await response.json();
                throw new Error(errData.error || 'Export failed');
            }
            if (!response.ok) {
                throw new Error('Export failed');
            }

            const blob = await response.blob();
            const disposition = response.headers.get('content-disposition') || '';
            let filename = 'Market_Data.zip';
            const match = disposition.match(/filename="?([^";]+)"?/i);
            if (match && match[1]) {
                filename = match[1];
            } else if (contentType.includes('sheet')) {
                filename = 'Market_Cap.xlsx';
            }

            const { filename: savedName, pickerUsed } = await saveBlobToChosenLocation(blob, filename);
            if (pickerUsed) {
                setSuccess(`✅ Excel export ready (${savedName})`);
            }

            // Only enable dashboard for range exports (not single date)
            if (scope === 'range' && rangeStartDate && rangeEndDate) {
                setConsolidationReady(true);
                setExportedRange({ start: rangeStartDate, end: rangeEndDate });
                setConsolidationStatus({
                    ready: true,
                    message: `Averages calculated for ${rangeStartDate} to ${rangeEndDate}`
                });
            }
        } catch (err) {
            setError(err.message);
            setExportLog([]);
        } finally {
            setExportLoading(false);
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
            const response = await fetch(`${VITE_API_URL}/api/preview`, {
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
        // Only local download

        try {
            const response = await fetch(`${VITE_API_URL}/api/consolidate`, {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Download failed');
            }

            // Only local download - response is binary file
            const blob = await response.blob();
            const { filename: savedName, pickerUsed } = await saveBlobToChosenLocation(blob, 'Finished_Product.xlsx');
            if (pickerUsed) {
                setSuccess(`✅ Excel file saved as ${savedName}`);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const loadDashboardData = async (limit) => {
        setDashboardLoading(true);
        setDashboardError(null);
        try {
            const response = await fetch(`${VITE_API_URL}/api/dashboard-data?limit=${limit}`);
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

    const handleUpdateIndices = async () => {
        setIndicesLoading(true);
        setError(null);
        setSuccess(null);

        try {
            const response = await fetch(`${VITE_API_URL}/api/update-indices`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({})
            });

            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || 'Index update failed');
            }

            setSuccess(`✅ Indices updated for ${data.count} symbols`);
            if (data.download_path) {
                window.open(`${VITE_API_URL}${data.download_path}`, '_blank');
            }

            if (activeTab === 'mongo') {
                await loadDashboardData(dashboardLimit);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setIndicesLoading(false);
        }
    };

    const handleBuildDashboard = async () => {
        // Dashboard only available for date range (after MCAP/PR averages are calculated)
        if (!rangeStartDate || !rangeEndDate) {
            setError('Select a date range first (Start Date and End Date)');
            return;
        }

        setDashboardLoading(true);
        setError(null);
        setSuccess(null);
        setDashboardResult(null);

        // Configuration for batch processing
        const TOTAL_SYMBOLS = 1000;
        const BATCH_SIZE = 100;
        const TOTAL_BATCHES = 10;

        let allRows = [];
        let allErrors = [];
        let processedSymbols = 0;

        try {
            for (let batchNum = 0; batchNum < TOTAL_BATCHES; batchNum++) {
                const startSymbol = batchNum * BATCH_SIZE + 1;
                const endSymbol = Math.min((batchNum + 1) * BATCH_SIZE, TOTAL_SYMBOLS);

                setDashboardBatchProgress({
                    currentBatch: batchNum + 1,
                    totalBatches: TOTAL_BATCHES,
                    symbolsProcessed: processedSymbols,
                    totalSymbols: TOTAL_SYMBOLS,
                    status: `Fetching symbols ${startSymbol}-${endSymbol}...`
                });

                const payload = {
                    batch_index: batchNum,
                    save_to_file: false, // Don't save file in backend batch
                    top_n: TOTAL_SYMBOLS,
                    top_n_by: 'mcap',
                    start_date: rangeStartDate, // Use raw YYYY-MM-DD
                    end_date: rangeEndDate     // Use raw YYYY-MM-DD
                };

                const response = await fetch(`${VITE_API_URL}/api/nse-symbol-dashboard`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(payload)
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    allErrors.push({ batch: batchNum + 1, error: errorData.error || 'Request failed' });
                    continue;
                }

                const data = await response.json();
                let batchCount = 0;
                if (data.rows) {
                    allRows = allRows.concat(data.rows);
                    batchCount = data.rows.length;
                }
                if (data.errors) {
                    allErrors = allErrors.concat(data.errors);
                }

                processedSymbols += batchCount;
                setDashboardBatchProgress({
                    currentBatch: batchNum + 1,
                    totalBatches: TOTAL_BATCHES,
                    symbolsProcessed: processedSymbols,
                    totalSymbols: TOTAL_SYMBOLS,
                    status: `✅ Batch ${batchNum + 1} complete (${batchCount} symbols)`
                });
            }

            // After all batches, send allRows to /api/nse-symbol-dashboard/save-excel
            let lastFileId = null;
            let lastDownloadUrl = null;
            let fileName = `Symbol_Dashboard_${convertDateFormat(rangeStartDate)}_${convertDateFormat(rangeEndDate)}.xlsx`;
            if (allRows.length > 0) {
                const saveExcelResp = await fetch(`${VITE_API_URL}/api/nse-symbol-dashboard/save-excel`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ rows: allRows, as_on: rangeEndDate })
                });
                if (saveExcelResp.ok) {
                    const saveExcelData = await saveExcelResp.json();
                    lastFileId = saveExcelData.file_id;
                    lastDownloadUrl = saveExcelData.download_url;
                    if (saveExcelData.file) fileName = saveExcelData.file;
                }
            }

            // Calculate averages from all collected rows
            const averages = {};
            if (allRows.length > 0) {
                ['impact_cost', 'free_float_mcap', 'total_market_cap', 'total_traded_value', 'last_price'].forEach(field => {
                    const values = allRows.map(r => r[field]).filter(v => v != null && !isNaN(v));
                    if (values.length > 0) {
                        averages[field] = (values.reduce((a, b) => a + b, 0) / values.length).toFixed(2);
                    }
                });
            }

            setDashboardResult({
                success: true,
                count: allRows.length,
                symbols_used: allRows.length,
                total_symbols: TOTAL_SYMBOLS,
                averages: averages,
                errors: allErrors,
                file_id: lastFileId,
                download_url: lastDownloadUrl,
                file: fileName
            });
            setSuccess(`✅ Dashboard complete! ${allRows.length} symbols fetched in ${TOTAL_BATCHES} batches`);
        } catch (err) {
            setError(err.message);
        } finally {
            setDashboardLoading(false);
            setDashboardBatchProgress(null);
        }
    };

    const handleDownloadDashboard = async () => {
        if (!dashboardResult || !dashboardResult.download_url) {
            setError('No dashboard available to download');
            return;
        }

        try {
            const response = await fetch(`${VITE_API_URL}${dashboardResult.download_url}`);
            if (!response.ok) {
                const ct = response.headers.get('content-type') || '';
                if (ct.includes('application/json')) {
                    const errData = await response.json();
                    throw new Error(errData.error || 'Dashboard download failed');
                }
                throw new Error('Dashboard download failed');
            }

            const blob = await response.blob();
            const { filename: savedName, pickerUsed } = await saveBlobToChosenLocation(blob, dashboardResult.file || 'Symbol_Dashboard.xlsx');
            if (pickerUsed) {
                setSuccess(`✅ Dashboard saved as ${savedName}`);
            }
        } catch (err) {
            setError(err.message);
        }
    };

    const handleFullPipeline = async () => {
        if (!rangeStartDate || !rangeEndDate) {
            setError('Please select both start and end dates');
            return;
        }

        if (rangeStartDate > rangeEndDate) {
            setError('Start date cannot be after end date');
            return;
        }

        setPipelineLoading(true);
        setError(null);
        setSuccess(null);
        setRangeProgress(null);
        setDashboardResult(null);

        try {
            // Step 1: Download Range
            setPipelineStage('Downloading data from NSE...');
            const downloadRes = await fetch(`${VITE_API_URL}/api/download-nse-range`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    start_date: convertDateFormat(rangeStartDate),
                    end_date: convertDateFormat(rangeEndDate),
                    save_to_file: true,
                    refresh_mode: 'missing_only'
                })
            });

            if (!downloadRes.ok) {
                const errorData = await downloadRes.json();
                throw new Error(`Download failed: ${errorData.error}`);
            }

            const downloadData = await downloadRes.json();
            setRangeProgress({
                summary: downloadData.summary,
                entries: downloadData.entries || [],
                errors: downloadData.errors || []
            });

            // Step 2: Export Consolidated (calculates averages)
            setPipelineStage('Calculating averages (Optimized)...');
            const exportRes = await fetch(`${VITE_API_URL}/api/consolidate-saved`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    start_date: convertDateFormat(rangeStartDate),
                    end_date: convertDateFormat(rangeEndDate),
                    file_type: 'both',
                    fast_mode: false,
                    skip_daily: true
                })
            });

            if (!exportRes.ok) {
                const errData = await exportRes.json();
                throw new Error(`Consolidation failed: ${errData.error}`);
            }

            const headerLog = exportRes.headers.get('x-export-log');
            if (headerLog) setExportLog(headerLog.split('\n'));

            setConsolidationReady(true);
            setExportedRange({ start: rangeStartDate, end: rangeEndDate });
            setConsolidationStatus({
                ready: true,
                message: `Averages calculated for ${rangeStartDate} to ${rangeEndDate}`
            });

            // Step 3: Build Dashboard
            setPipelineStage('Building Symbol Dashboard...');
            await handleBuildDashboard();

            setSuccess('✅ Full pipeline completed successfully!');
        } catch (err) {
            setError(err.message);
        } finally {
            setPipelineLoading(false);
            setPipelineStage('');
        }
    };

    return (
        <div className="app">
            <TabBar activeTab={activeTab} setActiveTab={setActiveTab} />

            <div className="content-area">
                <Header />

                <main className="main-content">
                    <AlertMessages
                        error={error}
                        success={success}
                        onErrorClose={() => setError(null)}
                        onSuccessClose={() => setSuccess(null)}
                    />

                    {activeTab === 'download' && (
                        <DownloadTab
                            nseDate={nseDate}
                            setNseDate={setNseDate}
                            nseLoading={nseLoading}
                            exportLoading={exportLoading}
                            exportLog={exportLog}
                            handleDownloadFromNSE={handleDownloadFromNSE}
                            handleExportConsolidated={handleExportConsolidated}
                        />
                    )}

                    {activeTab === 'download' && (
                        <DownloadTab
                            nseDate={nseDate}
                            setNseDate={setNseDate}
                            nseLoading={nseLoading}
                            exportLoading={exportLoading}
                            exportLog={exportLog}
                            handleDownloadFromNSE={handleDownloadFromNSE}
                            handleExportConsolidated={handleExportConsolidated}
                        />
                    )}

                    {activeTab === 'range' && (
                        <RangeTab
                            rangeStartDate={rangeStartDate}
                            setRangeStartDate={setRangeStartDate}
                            rangeEndDate={rangeEndDate}
                            setRangeEndDate={setRangeEndDate}
                            rangeLoading={rangeLoading}
                            rangeProgress={rangeProgress}
                            exportLoading={exportLoading}
                            exportLog={exportLog}
                            consolidationReady={consolidationReady}
                            exportedRange={exportedRange}
                            dashboardLoading={dashboardLoading}
                            dashboardResult={dashboardResult}
                            dashboardBatchProgress={dashboardBatchProgress}
                            handleDownloadRangeFromNSE={handleDownloadRangeFromNSE}
                            handleExportConsolidated={handleExportConsolidated}
                            handleBuildDashboard={handleBuildDashboard}
                            handleDownloadDashboard={handleDownloadDashboard}
                            handleFullPipeline={handleFullPipeline}
                            pipelineLoading={pipelineLoading}
                            pipelineStage={pipelineStage}
                        />
                    )}
                </main>
            </div>
        </div>
    );
}

export default App;