import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

export default defineConfig({
  plugins: [solid()],
  server: {
    port: 5173,
    proxy: {
      "/admin": "http://127.0.0.1:8000",
      "/health": "http://127.0.0.1:8000",
      "/v1": "http://127.0.0.1:8000",
    },
  },
  build: {
    target: "es2022",
    sourcemap: false,
  },
});
