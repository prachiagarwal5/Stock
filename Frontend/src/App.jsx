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
    const [consolidationBatchProgress, setConsolidationBatchProgress] = useState(null);
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
        // Handle ISO strings from new Date().toISOString()
        const normalized = dateStr.includes('T') ? dateStr.split('T')[0] : dateStr;
        const [year, month, day] = normalized.split('-');
        const date = new Date(year, parseInt(month) - 1, parseInt(day));
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        return `${day}-${months[date.getMonth()]}-${year}`;
    };

    const getTradingDates = (startStr, endStr) => {
        const start = new Date(startStr);
        const end = new Date(endStr);
        const dates = [];
        let curr = new Date(start);

        while (curr <= end) {
            const day = curr.getDay();
            if (day !== 0 && day !== 6) { // 0=Sun, 6=Sat
                dates.push(new Date(curr));
            }
            curr.setDate(curr.getDate() + 1);
        }
        return dates;
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

        const tradingDates = getTradingDates(rangeStartDate, rangeEndDate);
        if (tradingDates.length === 0) {
            setError('No trading days found in the selected range');
            return;
        }

        setRangeLoading(true);
        setError(null);
        setSuccess(null);

        const initialProgress = {
            summary: {
                total_requested: tradingDates.length,
                cached: 0,
                fetched: 0,
                failed: 0,
                percentage: 0
            },
            entries: [],
            errors: []
        };
        setRangeProgress(initialProgress);

        const BATCH_SIZE = 40;
        let cumulativeSummary = { ...initialProgress.summary };
        let cumulativeEntries = [];
        let cumulativeErrors = [];

        try {
            for (let i = 0; i < tradingDates.length; i += BATCH_SIZE) {
                const batch = tradingDates.slice(i, i + BATCH_SIZE);
                const batchStart = batch[0].toISOString().split('T')[0];
                const batchEnd = batch[batch.length - 1].toISOString().split('T')[0];

                const response = await fetch(`${VITE_API_URL}/api/download-nse-range`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        start_date: convertDateFormat(batchStart),
                        end_date: convertDateFormat(batchEnd),
                        save_to_file: true,
                        refresh_mode: 'missing_only',
                        parallel_workers: 20
                    })
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.error || `Batch ${Math.floor(i / BATCH_SIZE) + 1} failed`);
                }

                const data = await response.json();

                // Aggregate results
                cumulativeSummary.cached += (data.summary.cached || 0);
                cumulativeSummary.fetched += (data.summary.fetched || 0);
                cumulativeSummary.failed += (data.summary.failed || 0);
                cumulativeSummary.percentage = Math.round(((i + batch.length) / tradingDates.length) * 100);
                cumulativeEntries = [...cumulativeEntries, ...(data.entries || [])];
                cumulativeErrors = [...cumulativeErrors, ...(data.errors || [])];

                setRangeProgress({
                    summary: cumulativeSummary,
                    entries: cumulativeEntries,
                    errors: cumulativeErrors
                });
            }

            const message = cumulativeSummary.fetched === 0 && cumulativeSummary.failed === 0
                ? `✅ All ${cumulativeSummary.total_requested} days served from cache`
                : `✅ Ready: cached ${cumulativeSummary.cached}, fetched ${cumulativeSummary.fetched}, total ${cumulativeSummary.total_requested}`;
            setSuccess(message);
        } catch (err) {
            setError(err.message);
        } finally {
            setRangeLoading(false);
        }
    };

    const handleExportConsolidated = async (scope = 'date', skipDaily = true) => {
        setExportLog(['Starting export...']);
        setError(null);
        setSuccess(null);
        setConsolidationBatchProgress(null);

        if (scope === 'range' && (!rangeStartDate || !rangeEndDate)) {
            setError('Select a start and end date');
            setExportLog([]);
            return;
        }

        if (scope === 'date' && !nseDate) {
            setError('Select a date to export');
            setExportLog([]);
            return;
        }

        setExportLoading(true);

        // Initialize progress for better visibility
        setConsolidationBatchProgress({
            current: 0,
            total: 100,
            percentage: 5,
            message: 'Initializing consolidation...'
        });

        try {
            if (scope === 'range') {
                const tradingDates = getTradingDates(rangeStartDate, rangeEndDate);
                const BATCH_SIZE = 40;

                if (tradingDates.length > BATCH_SIZE) {
                    console.log(`[consolidation] Large range detected (${tradingDates.length} days). Batching consolidation...`);

                    for (let i = 0; i < tradingDates.length; i += BATCH_SIZE) {
                        const batch = tradingDates.slice(i, i + BATCH_SIZE);
                        const batchStart = batch[0].toISOString().split('T')[0];
                        const batchEnd = batch[batch.length - 1].toISOString().split('T')[0];

                        const progress = {
                            current: Math.min(i + BATCH_SIZE, tradingDates.length),
                            total: tradingDates.length,
                            percentage: Math.round((Math.min(i + BATCH_SIZE, tradingDates.length) / tradingDates.length) * 100),
                            message: `Consolidating ${convertDateFormat(batchStart)} to ${convertDateFormat(batchEnd)}...`
                        };
                        setConsolidationBatchProgress(progress);
                        setExportLog(prev => [...prev, progress.message]);

                        // Call backend with fast_mode: false to persist averages for this sub-range
                        // This "warms up" the DB with symbol_daily data if it wasn't already there
                        // although the user should have "Downloaded Range" first.
                        await fetch(`${VITE_API_URL}/api/consolidate-saved`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                start_date: convertDateFormat(batchStart),
                                end_date: convertDateFormat(batchEnd),
                                file_type: 'mcap', // Just mcap is enough to warm up common data
                                fast_mode: false,
                                skip_daily: true
                            })
                        });
                    }
                    setConsolidationBatchProgress({
                        current: tradingDates.length,
                        total: tradingDates.length,
                        percentage: 100,
                        message: 'Finalizing full consolidation...'
                    });
                }
            }

            // Final consolidation for the full range (or single date)
            setConsolidationBatchProgress(prev => ({
                ...prev,
                percentage: Math.max(prev?.percentage || 0, 90),
                message: 'Generating final consolidation Excel files...'
            }));

            const finalPayload = {
                file_type: 'both',
                fast_mode: false,
                skip_daily: skipDaily
            };

            if (scope === 'range') {
                finalPayload.start_date = convertDateFormat(rangeStartDate);
                finalPayload.end_date = convertDateFormat(rangeEndDate);
            } else {
                finalPayload.date = convertDateFormat(nseDate);
            }

            const response = await fetch(`${VITE_API_URL}/api/consolidate-saved`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(finalPayload)
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
            // Don't clear export log on error, it might have useful info
        } finally {
            setExportLoading(true); // Keep spinner if still doing something? No, set to false.
            setExportLoading(false);
            setConsolidationBatchProgress(null);
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
        if (!rangeStartDate || !rangeEndDate) {
            setError('Select a date range first (Start Date and End Date)');
            return;
        }

        setDashboardLoading(true);
        setError(null);
        setSuccess(null);
        setDashboardResult(null);

        const TOTAL_SYMBOLS = 1100;

        try {
            setDashboardBatchProgress({
                currentBatch: 1,
                totalBatches: 1,
                symbolsProcessed: 0,
                totalSymbols: TOTAL_SYMBOLS,
                status: 'Connecting to dashboard stream...',
                message: 'Initializing...',
                percentage: 0
            });

            const response = await fetch(`${VITE_API_URL}/api/nse-symbol-dashboard`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    top_n: TOTAL_SYMBOLS,
                    top_n_by: 'mcap',
                    as_on: rangeEndDate,
                    start_date: rangeStartDate,
                    end_date: rangeEndDate
                })
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Dashboard generation failed');
            }

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';

            while (true) {
                const { value, done } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop();

                for (const line of lines) {
                    if (line.startsWith('data: ')) {
                        try {
                            const data = JSON.parse(line.substring(6));
                            if (data.error) throw new Error(data.error);

                            if (data.message) {
                                setDashboardBatchProgress(prev => ({
                                    ...prev,
                                    message: data.message,
                                    percentage: data.percentage !== undefined ? data.percentage : prev.percentage
                                }));
                                setExportLog(prev => [...prev, data.message]);
                            }

                            if (data.complete) {
                                setDashboardResult(data);
                                setSuccess(`✅ Dashboard built successfully with ${data.count} symbols`);
                                if (data.rows && data.rows.length > 0 && activeTab === 'mongo') {
                                    await loadDashboardData(dashboardLimit);
                                }
                                break;
                            }
                        } catch (e) {
                            console.error("Error parsing dashboard SSE:", e);
                        }
                    }
                }
            }
        } catch (err) {
            console.error('Dashboard Error:', err);
            setError(err.message);
            setDashboardBatchProgress(null);
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
            // Step 1: Download Range (Batched to avoid Vercel timeout)
            setPipelineStage('Downloading data from NSE (Batched)...');

            const tradingDates = getTradingDates(rangeStartDate, rangeEndDate);
            if (tradingDates.length === 0) {
                throw new Error('No trading days found in the selected range');
            }

            const initialProgress = {
                summary: {
                    total_requested: tradingDates.length,
                    cached: 0,
                    fetched: 0,
                    failed: 0,
                    percentage: 0
                },
                entries: [],
                errors: []
            };
            setRangeProgress(initialProgress);

            const BATCH_SIZE = 40;
            let cumulativeSummary = { ...initialProgress.summary };
            let cumulativeEntries = [];
            let cumulativeErrors = [];

            for (let i = 0; i < tradingDates.length; i += BATCH_SIZE) {
                const batch = tradingDates.slice(i, i + BATCH_SIZE);
                const batchStart = batch[0].toISOString().split('T')[0];
                const batchEnd = batch[batch.length - 1].toISOString().split('T')[0];

                const downloadRes = await fetch(`${VITE_API_URL}/api/download-nse-range`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        start_date: convertDateFormat(batchStart),
                        end_date: convertDateFormat(batchEnd),
                        save_to_file: true,
                        refresh_mode: 'missing_only',
                        parallel_workers: 20
                    })
                });

                if (!downloadRes.ok) {
                    const errorData = await downloadRes.json();
                    throw new Error(`Download failed at batch ${Math.floor(i / BATCH_SIZE) + 1}: ${errorData.error}`);
                }

                const downloadData = await downloadRes.json();

                // Aggregate
                cumulativeSummary.cached += (downloadData.summary.cached_count || 0);
                cumulativeSummary.fetched += (downloadData.summary.fetched_count || 0);
                cumulativeSummary.failed += (downloadData.summary.failed_count || 0);
                cumulativeSummary.percentage = Math.round(((i + batch.length) / tradingDates.length) * 100);
                cumulativeEntries = [...cumulativeEntries, ...(downloadData.entries || [])];
                cumulativeErrors = [...cumulativeErrors, ...(downloadData.errors || [])];

                setRangeProgress({
                    summary: cumulativeSummary,
                    entries: cumulativeEntries,
                    errors: cumulativeErrors
                });
            }

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
                            consolidationBatchProgress={consolidationBatchProgress}
                        />
                    )}
                </main>
            </div>
        </div>
    );
}

export default App;