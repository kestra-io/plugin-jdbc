import defaultViteConfig from "@kestra-io/artifact-sdk/vite.config";
import types from "./jdbc-types.json" with { type: "json" };
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// The host asks one container per group for `./<SimpleClassName>/<slot>`, so Gradle builds once per group (JDBC_UI_GROUP).
const group = process.env.JDBC_UI_GROUP;
const classes = (types as Record<string, string[]>)[group ?? ""];
if (!group) {
    // generic `npm run build` (e.g. kestra-plugin-shadowjar): Gradle builds each sub-plugin UI itself
    console.log("JDBC_UI_GROUP not set, skipping: Gradle builds one UI per sub-plugin module");
    process.exit(0);
}
if (!classes) {
    throw new Error(`Set JDBC_UI_GROUP to one of: ${Object.keys(types).join(", ")}`);
}
const plugin = `io.kestra.plugin.jdbc.${group}`;

// topology-details: 2 rows without execution, 4 rows with one (node base 56px + ~31px per row).
const exposes = Object.fromEntries(
    classes.map((type) => [
        type,
        [
            {
                slotName: "topology-details" as const,
                path: "./src/components/JdbcTopologyDetails.vue",
                additionalProperties: {
                    height: 120,
                    heightWithExecution: 185,
                },
            },
            {
                slotName: "topology-task-drawer" as const,
                path: "./src/components/JdbcTopologyDetails.vue",
            },
            {
                slotName: "topology-task-modal" as const,
                path: "./src/components/JdbcTaskModal.vue",
            },
        ],
    ]),
);

// The SDK names the container after the working directory's package.json, hence a throwaway one.
const root = process.cwd();
const nameDir = mkdtempSync(join(tmpdir(), "jdbc-ui-"));
writeFileSync(join(nameDir, "package.json"), JSON.stringify({ name: plugin }));
process.chdir(nameDir);
const config = defaultViteConfig({ plugin, exposes });
process.chdir(root);
rmSync(nameDir, { recursive: true, force: true });

export default config;
