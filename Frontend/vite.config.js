import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
    plugins: [react()],
    server: {
        port: 5173,
        open: false,
        // Proxy API requests to backend
        proxy: {
            '/api': {
                target: process.env.VITE_API_URL,
                changeOrigin: true,
                rewrite: (path) => path
            }
        }
    },
    build: {
        outDir: 'dist',
        sourcemap: false,
        minify: 'terser'
    },
    define: {
        __DEV__: JSON.stringify(process.env.NODE_ENV === 'development')
    }
})
