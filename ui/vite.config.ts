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

// topology-details: 2 rows before an execution (node header 56px + ~31px per row, kept as is), 4 rows after one.
// heightWithExecution is a flat value instead of height + N: the card never exceeds 4 rows, and the measured
// post-execution layout is 58px header + 4 rows of ~33.5px = 192px, plus 1px margin.
const POST_EXECUTION_HEIGHT = 193;
const exposes = Object.fromEntries(
    classes.map((type) => [
        type,
        [
            {
                slotName: "topology-details" as const,
                path: "./src/components/JdbcTopologyDetails.vue",
                additionalProperties: {
                    height: 120,
                    heightWithExecution: POST_EXECUTION_HEIGHT,
                    customAction: { label: "Show Details", taskProp: "sql", lang: "sql" },
                },
            },
            {
                slotName: "topology-task-drawer" as const,
                path: "./src/components/JdbcTopologyDetails.vue",
            },
            {
                slotName: "execution-artifact/details" as const,
                path: "./src/components/JdbcTaskModal.vue",
                additionalProperties: { title: "Execution details" },
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
