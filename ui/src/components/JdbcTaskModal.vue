<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { computed, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { KsTopologyDetails } from "@kestra-io/design-system";
import { DASH, formatBytes, formatDuration, formatNumber, safeHref } from "../lib/details";

const { t } = useI18n({
    inheritLocale: true,
    useScope: "local",
    messages: {
        en: {
            execution: "Execution",
            status: "Status",
            table: "Table",
            access: "Access",
            confidence: "Confidence",
            tablesNote: "Detected from the SQL text, not a full parser. CTEs and dialect-specific syntax can be missed.",
            statementNote: "Secret values appear as ***. Truncated at 64 KiB.",
            download: "Open",
            rowsAffected: "Rows affected",
            rowsFetched: "Rows fetched",
            storedSize: "Stored size",
            duration: "Duration",
            warnings: "Warnings",
            tables: "Tables",
            tablesTruncated: "The list is truncated to 50 tables.",
            noTables: "No table detected.",
            statement: "Statement",
            files: "Files",
            results: "Results",
            statementsFile: "Per statement details",
            columnStats: "Column statistics",
            noExecution: "No execution yet.",
        },
    },
});

const props = defineProps<KnownSlotProps["topology-task-modal"]>();

const taskRun = computed(() => {
    const list = props.execution?.taskRunList as any[] | undefined;
    return list?.filter((tr: any) => tr.taskId === props.task?.id).at(-1);
});

const outputs = ref<Record<string, any> | null>(null);

async function loadOutputs() {
    try {
        outputs.value = (await props.fetchOutputs?.({ taskRunId: taskRun.value?.id })) ?? null;
    } catch {
        /* best-effort */
    }
}

watch(
    () => props.execution?.id,
    (id) => {
        if (id) loadOutputs();
    },
    { immediate: true },
);

const data = computed(() => outputs.value ?? taskRun.value?.outputs ?? null);

const executionRows = computed(() => {
    const o = data.value ?? {};
    const rows = [
        typeof o.rowsAffected === "number"
            ? { label: t("rowsAffected"), value: formatNumber(o.rowsAffected) }
            : { label: t("rowsFetched"), value: formatNumber(o.size) },
        { label: t("storedSize"), value: formatBytes(o.bytes) },
        { label: t("duration"), value: formatDuration(o.durationMs) },
        { label: t("warnings"), value: formatNumber(o.warnings) },
    ];
    return rows;
});

const state = computed(() => (taskRun.value?.state?.current as string | undefined) ?? undefined);
const accessClass = (access?: string) =>
    access === "READ" ? "ok" : access === "WRITE" ? "info" : access ? "warn" : "";

const tables = computed(() => (Array.isArray(data.value?.tables) ? (data.value!.tables as any[]) : []));
const tableName = (table: any) =>
    [table.database, table.schema, table.name].filter((part) => typeof part === "string").join(".");

const files = computed(() =>
    (
        [
            ["uri", t("results")],
            ["statements", t("statementsFile")],
            ["columnStats", t("columnStats")],
        ] as const
    )
        .filter(([key]) => typeof data.value?.[key] === "string")
        .map(([key, label]) => ({ key, label, uri: data.value![key] as string })),
);
</script>

<template>
    <div class="jdbc-modal">
        <p v-if="!data" class="jdbc-modal__empty">{{ t("noExecution") }}</p>
        <template v-else>
            <section class="jdbc-modal__section">
                <h4 class="jdbc-modal__title">{{ t("execution") }}</h4>
                <div v-if="state" class="jdbc-modal__kv">
                    <span>{{ t("status") }}</span>
                    <span class="jdbc-modal__pill" :class="state === 'SUCCESS' ? 'ok' : state === 'WARNING' ? 'warn' : 'info'">{{ state.charAt(0) + state.slice(1).toLowerCase() }}</span>
                </div>
                <KsTopologyDetails :rows="executionRows" />
            </section>

            <section class="jdbc-modal__section">
                <h4 class="jdbc-modal__title">{{ t("tables") }}</h4>
                <p v-if="tables.length === 0" class="jdbc-modal__empty">{{ t("noTables") }}</p>
                <div v-else class="jdbc-modal__scroll">
                    <table class="jdbc-modal__table">
                        <thead>
                            <tr>
                                <th>{{ t("table") }}</th>
                                <th>{{ t("access") }}</th>
                                <th>{{ t("confidence") }}</th>
                                <th v-if="tables.some((table) => table.kind)" />
                            </tr>
                        </thead>
                        <tbody>
                            <tr v-for="(table, index) in tables" :key="index">
                                <td class="jdbc-modal__mono">{{ tableName(table) }}</td>
                                <td><span class="jdbc-modal__pill" :class="accessClass(table.access)">{{ table.access ?? DASH }}</span></td>
                                <td>{{ table.confidence ? table.confidence.charAt(0) + table.confidence.slice(1).toLowerCase() : DASH }}</td>
                                <td v-if="tables.some((other) => other.kind)" class="jdbc-modal__dim">{{ table.kind ?? "" }}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
                <p v-if="tables.length" class="jdbc-modal__note">{{ t("tablesNote") }}</p>
                <p v-if="data.tablesTruncated" class="jdbc-modal__empty">{{ t("tablesTruncated") }}</p>
            </section>

            <section v-if="typeof data.statement === 'string'" class="jdbc-modal__section">
                <h4 class="jdbc-modal__title">{{ t("statement") }}</h4>
                <pre class="jdbc-modal__code">{{ data.statement }}</pre>
                <p class="jdbc-modal__note">{{ t("statementNote") }}</p>
            </section>

            <section v-if="files.length" class="jdbc-modal__section">
                <h4 class="jdbc-modal__title">{{ t("files") }}</h4>
                <div v-for="file in files" :key="file.key" class="jdbc-modal__kv">
                    <span>{{ file.label }}</span>
                    <a v-if="safeHref(file.uri)" class="jdbc-modal__link" :href="safeHref(file.uri)" target="_blank" rel="noopener noreferrer">{{ t("download") }} ↗</a>
                    <code v-else class="jdbc-modal__mono">{{ file.uri }}</code>
                </div>
            </section>
        </template>
    </div>
</template>

<style scoped>
/* Tokens follow the details mockup. Host variables are not guaranteed in this slot. */
.jdbc-modal {
    --jm-canvas: #f1f2f7;
    --jm-border: #dcdee8;
    --jm-text: #1b1d2a;
    --jm-dim: #5d6178;
    --jm-accent: #8405ff;
    --jm-accent-soft: #efe3ff;
    --jm-ok: #17874f;
    --jm-ok-bg: #dff5e8;
    --jm-warn: #a8630a;
    --jm-warn-bg: #fdecd0;
    padding-bottom: 16px;
    font-size: 13px;
    color: var(--jm-text);
}

:global(html.dark) .jdbc-modal,
:global(.dark) .jdbc-modal {
    --jm-canvas: #10121a;
    --jm-border: #2a2d3c;
    --jm-text: #f2f3f8;
    --jm-dim: #a3a7bd;
    --jm-accent: #a45cff;
    --jm-accent-soft: #2b1d45;
    --jm-ok: #46d28b;
    --jm-ok-bg: #12301f;
    --jm-warn: #f5b25c;
    --jm-warn-bg: #3a2810;
}

.jdbc-modal__section {
    margin: 20px 16px 0;
}

.jdbc-modal__title {
    margin: 0 0 4px;
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--jm-dim);
}

.jdbc-modal__empty,
.jdbc-modal__note {
    margin: 8px 0 0;
    color: var(--jm-dim);
    font-size: 12px;
}

.jdbc-modal__dim {
    color: var(--jm-dim);
}

.jdbc-modal__mono {
    font-family: ui-monospace, "JetBrains Mono", "SFMono-Regular", Menlo, monospace;
    font-size: 12.5px;
}

.jdbc-modal__kv {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    padding: 9px 12px;
    border-bottom: 1px solid var(--jm-border);
}

.jdbc-modal__kv > :first-child {
    color: var(--jm-dim);
}

.jdbc-modal__pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 3px 8px;
    border-radius: 6px;
    background: var(--jm-ok-bg);
    color: var(--jm-ok);
    font-size: 12px;
    font-weight: 600;
}

.jdbc-modal__pill::before {
    content: "";
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: currentColor;
}

.jdbc-modal__pill.info {
    background: var(--jm-accent-soft);
    color: var(--jm-accent);
}

.jdbc-modal__pill.warn {
    background: var(--jm-warn-bg);
    color: var(--jm-warn);
}

.jdbc-modal__link {
    color: var(--jm-accent);
}

.jdbc-modal__scroll {
    overflow-x: auto;
}

.jdbc-modal__table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}

.jdbc-modal__table th {
    padding: 6px 12px;
    border-bottom: 1px solid var(--jm-border);
    text-align: left;
    font-weight: 500;
    color: var(--jm-dim);
}

.jdbc-modal__table td {
    padding: 8px 12px;
    border-bottom: 1px solid var(--jm-border);
}

.jdbc-modal__table tr:last-child td {
    border-bottom: 0;
}

.jdbc-modal__code {
    max-height: 20rem;
    margin: 8px 0 0;
    overflow: auto;
    padding: 12px;
    border: 1px solid var(--jm-border);
    border-radius: 6px;
    background: var(--jm-canvas);
    font-family: ui-monospace, "JetBrains Mono", "SFMono-Regular", Menlo, monospace;
    font-size: 12px;
    white-space: pre-wrap;
}
</style>
