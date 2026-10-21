import { defineConfig } from "vitest/config";

// Kept apart from vite.config.ts, which needs JDBC_UI_GROUP to build a federated container.
export default defineConfig({
    test: { include: ["tests/**/*.spec.ts"] },
});
