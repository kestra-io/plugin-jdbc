import defaultViteConfig from "@kestra-io/artifact-sdk/vite.config";
import types from "./jdbc-types.json" with { type: "json" };

// Every Query, Queries and Batch type of every JDBC sub-plugin gets the same two components, so the
// exposes map is generated from jdbc-types.json (`<package>.<Class>` under io.kestra.plugin.jdbc).
// topology-details: 2 rows without execution, 4 rows with one (node base 56px + ~31px per row).
const exposes = Object.fromEntries(
    (types as string[]).map((type) => [
        type,
        [
            {
                slotName: "topology-details",
                path: "./src/components/JdbcTopologyDetails.vue",
                additionalProperties: {
                    height: 120,
                    heightWithExecution: 185,
                    customAction: { label: "Show Details", taskProp: "sql", lang: "sql" },
                },
            },
            {
                slotName: "topology-task-drawer",
                path: "./src/components/JdbcTopologyDetails.vue",
            },
            {
                slotName: "topology-task-modal",
                path: "./src/components/JdbcTaskModal.vue",
            },
        ],
    ]),
);

export default defaultViteConfig({
    plugin: "io.kestra.plugin.jdbc",
    exposes,
});
