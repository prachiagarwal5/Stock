// API Configuration
export const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000';

// App Constants
export const APP_NAME = 'Market Cap Consolidation Tool';
export const APP_TAGLINE = 'Professional market capitalization data consolidation made simple';

// File Upload Constants
export const ALLOWED_FILE_TYPES = '.csv';
export const MAX_FILE_SIZE = 50 * 1024 * 1024; // 50MB

// API Endpoints
export const ENDPOINTS = {
    PREVIEW: '/api/preview',
    CONSOLIDATE: '/api/consolidate',
    HEALTH: '/health'
};
