import React, { useState } from 'react';
import SummaryCard from './SummaryCard';
import './PreviewSection.css';

const PreviewSection = ({ preview }) => {
    const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });
    const [searchTerm, setSearchTerm] = useState('');

    if (!preview) {
        return (
            <section className="preview-section animate-slideInBottom">
                <div className="no-preview">
                    <div className="no-preview-icon">
                        <svg width="80" height="80" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                            <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
                            <circle cx="12" cy="12" r="3"></circle>
                        </svg>
                    </div>
                    <h3>No Preview Available</h3>
                    <p>Upload CSV files and click "Preview Data" to see the consolidated results</p>
                </div>
            </section>
        );
    }

    // Sorting logic
    const handleSort = (columnIndex) => {
        let direction = 'asc';
        if (sortConfig.key === columnIndex && sortConfig.direction === 'asc') {
            direction = 'desc';
        }
        setSortConfig({ key: columnIndex, direction });
    };

    // Get sorted data
    const getSortedData = () => {
        if (!sortConfig.key && sortConfig.key !== 0) return preview.preview.data;

        const sorted = [...preview.preview.data].sort((a, b) => {
            const aVal = a[sortConfig.key];
            const bVal = b[sortConfig.key];

            // Handle null/undefined values
            if (aVal === null || aVal === undefined) return 1;
            if (bVal === null || bVal === undefined) return -1;

            // Compare numbers
            if (typeof aVal === 'number' && typeof bVal === 'number') {
                return sortConfig.direction === 'asc' ? aVal - bVal : bVal - aVal;
            }

            // Compare strings
            const aStr = String(aVal).toLowerCase();
            const bStr = String(bVal).toLowerCase();

            if (sortConfig.direction === 'asc') {
                return aStr.localeCompare(bStr);
            }
            return bStr.localeCompare(aStr);
        });

        return sorted;
    };

    // Filter data based on search
    const getFilteredData = () => {
        const sortedData = getSortedData();

        if (!searchTerm.trim()) return sortedData;

        return sortedData.filter(row =>
            row.some(cell =>
                cell !== null &&
                cell !== undefined &&
                String(cell).toLowerCase().includes(searchTerm.toLowerCase())
            )
        );
    };

    const filteredData = getFilteredData();

    // Calculate statistics
    const calculateStats = () => {
        const marketCapColumns = preview.preview.columns.slice(2); // Skip Symbol and Company Name
        const stats = {
            avgMarketCap: 0,
            maxMarketCap: 0,
            minMarketCap: Infinity,
            totalCompanies: preview.summary.total_companies
        };

        let sum = 0;
        let count = 0;

        preview.preview.data.forEach(row => {
            row.slice(2).forEach(cell => {
                if (typeof cell === 'number' && cell > 0) {
                    sum += cell;
                    count++;
                    stats.maxMarketCap = Math.max(stats.maxMarketCap, cell);
                    stats.minMarketCap = Math.min(stats.minMarketCap, cell);
                }
            });
        });

        stats.avgMarketCap = count > 0 ? sum / count : 0;
        if (stats.minMarketCap === Infinity) stats.minMarketCap = 0;

        return stats;
    };

    const stats = calculateStats();

    return (
        <section className="preview-section animate-slideInBottom">
            <div className="section-header">
                <h2>📊 Data Preview & Analysis</h2>
                <p className="section-subtitle">Interactive analysis of your consolidated market cap data</p>
            </div>

            {/* Summary Cards */}
            <div className="summary-cards">
                <SummaryCard
                    icon="🏢"
                    label="Total Companies"
                    value={preview.summary.total_companies}
                    variant="primary"
                />
                <SummaryCard
                    icon="📅"
                    label="Total Dates"
                    value={preview.summary.total_dates}
                    variant="success"
                />
                <SummaryCard
                    icon="📤"
                    label="Files Processed"
                    value={preview.summary.uploaded_files}
                    variant="dark"
                />
            </div>

            {/* Statistics Cards */}
            <div className="stats-section">
                <h3 className="stats-title">📈 Market Cap Statistics</h3>
                <div className="stats-grid">
                    <div className="stat-card">
                        <div className="stat-label">Average Market Cap</div>
                        <div className="stat-value">
                            ₹{(stats.avgMarketCap / 10000000).toFixed(2)} Cr
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Highest Market Cap</div>
                        <div className="stat-value">
                            ₹{(stats.maxMarketCap / 10000000).toFixed(2)} Cr
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Lowest Market Cap</div>
                        <div className="stat-value">
                            ₹{(stats.minMarketCap / 10000000).toFixed(2)} Cr
                        </div>
                    </div>
                </div>
            </div>

            {/* Dates List */}
            <div className="dates-section">
                <h3 className="dates-title">📆 Dates Included ({preview.summary.dates.length})</h3>
                <div className="dates-tags">
                    {preview.summary.dates.map((date, idx) => (
                        <span key={idx} className="date-tag">
                            {date}
                        </span>
                    ))}
                </div>
            </div>

            {/* Search Bar */}
            <div className="search-section">
                <div className="search-bar">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <circle cx="11" cy="11" r="8"></circle>
                        <path d="m21 21-4.35-4.35"></path>
                    </svg>
                    <input
                        type="text"
                        placeholder="Search by company name or symbol..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        className="search-input"
                    />
                    {searchTerm && (
                        <button className="search-clear" onClick={() => setSearchTerm('')}>
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                <line x1="18" y1="6" x2="6" y2="18"></line>
                                <line x1="6" y1="6" x2="18" y2="18"></line>
                            </svg>
                        </button>
                    )}
                </div>
                <div className="search-info">
                    {searchTerm && (
                        <span>
                            Showing {filteredData.length} of {preview.preview.data.length} companies
                        </span>
                    )}
                </div>
            </div>

            {/* Preview Table */}
            <div className="preview-table-container">
                <div className="table-header">
                    <h3 className="table-title">📈 Complete Data View ({filteredData.length} Companies)</h3>
                    <div className="table-info">
                        <span className="info-badge">
                            💡 Click column headers to sort
                        </span>
                    </div>
                </div>
                <div className="table-wrapper">
                    <table className="preview-table">
                        <thead>
                            <tr>
                                {preview.preview.columns.map((col, idx) => (
                                    <th
                                        key={idx}
                                        onClick={() => handleSort(idx)}
                                        className={sortConfig.key === idx ? 'sorted' : ''}
                                    >
                                        <div className="th-content">
                                            <span>{col}</span>
                                            {sortConfig.key === idx && (
                                                <span className="sort-icon">
                                                    {sortConfig.direction === 'asc' ? '↑' : '↓'}
                                                </span>
                                            )}
                                        </div>
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {filteredData.length === 0 ? (
                                <tr>
                                    <td colSpan={preview.preview.columns.length} className="no-results">
                                        No companies found matching "{searchTerm}"
                                    </td>
                                </tr>
                            ) : (
                                filteredData.map((row, rowIdx) => (
                                    <tr key={rowIdx}>
                                        {row.map((cell, colIdx) => (
                                            <td key={colIdx} className={colIdx >= 2 ? 'number-cell' : ''}>
                                                {cell === null || cell === undefined
                                                    ? '—'
                                                    : typeof cell === 'number'
                                                        ? cell.toLocaleString('en-IN', {
                                                            minimumFractionDigits: 2,
                                                            maximumFractionDigits: 2,
                                                        })
                                                        : cell}
                                            </td>
                                        ))}
                                    </tr>
                                ))
                            )}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* Help Section */}
            <div className="help-section">
                <h4>📋 Quick Guide</h4>
                <ul>
                    <li>🔍 Use the search bar to find specific companies</li>
                    <li>↕️ Click on column headers to sort data (ascending/descending)</li>
                    <li>💰 Market cap values are displayed in Indian format (Lakhs/Crores)</li>
                    <li>📊 Statistics show average, highest, and lowest market cap across all dates</li>
                </ul>
            </div>
        </section>
    );
};

export default PreviewSection;
