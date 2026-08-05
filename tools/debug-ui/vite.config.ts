import react from '@vitejs/plugin-react';
import autoprefixer from 'autoprefixer';
import { defineConfig } from 'vite';

export default defineConfig({
    plugins: [react()],
    css: {
        postcss: {
            plugins: [autoprefixer()],
        },
    },
    build: {
        outDir: 'build',
        assetsDir: 'static',
        sourcemap: true,
    },
    server: {
        port: 3000,
        strictPort: true,
    },
});
