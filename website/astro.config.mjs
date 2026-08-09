import { defineConfig } from "astro/config";

export default defineConfig({
  site: "https://riffdb.com",
  output: "static",
  build: {
    format: "directory",
  },
  vite: {
    build: {
      cssMinify: "lightningcss",
    },
  },
});
