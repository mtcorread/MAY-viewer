import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";

// Build straight into the Python package so `mayviewer serve` ships it.
// `base: "./"` keeps asset URLs relative (works behind any path / CDN).
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: {
    outDir: resolve(__dirname, "../mayviewer/web/dist"),
    emptyOutDir: true,
    target: "es2022",
  },
  server: {
    // `npm run dev` proxies the cache + runtime config to a running serve.
    proxy: {
      "/cache": "http://127.0.0.1:8000",
      "/app-config.json": "http://127.0.0.1:8000",
    },
  },
});
