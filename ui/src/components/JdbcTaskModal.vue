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
                <KsTopologyDetails :rows="executionRows" />
            </section>

            <section class="jdbc-modal__section">
                <h4 class="jdbc-modal__title">{{ t("tables") }}</h4>
                <p v-if="tables.length === 0" class="jdbc-modal__empty">{{ t("noTables") }}</p>
                <table v-else class="jdbc-modal__table">
                    <tbody>
                        <tr v-for="(table, index) in tables" :key="index">
                            <td>{{ tableName(table) }}</td>
                            <td>{{ table.access ?? DASH }}</td>
                            <td>{{ table.confidence ?? DASH }}</td>
                            <td v-if="table.kind">{{ table.kind }}</td>
                        </tr>
                    </tbody>
                </table>
                <p v-if="data.tablesTruncated" class="jdbc-modal__empty">{{ t("tablesTruncated") }}</p>
            </section>

            <section v-if="typeof data.statement === 'string'" class="jdbc-modal__section">
                <h4 class="jdbc-modal__title">{{ t("statement") }}</h4>
                <pre class="jdbc-modal__code">{{ data.statement }}</pre>
            </section>

            <section v-if="files.length" class="jdbc-modal__section">
                <h4 class="jdbc-modal__title">{{ t("files") }}</h4>
                <ul class="jdbc-modal__files">
                    <li v-for="file in files" :key="file.key">
                        <span>{{ file.label }}: </span>
                        <a v-if="safeHref(file.uri)" :href="safeHref(file.uri)" target="_blank" rel="noopener noreferrer">{{ file.uri }}</a>
                        <code v-else>{{ file.uri }}</code>
                    </li>
                </ul>
            </section>
        </template>
    </div>
</template>

<style scoped>
.jdbc-modal__section {
    margin: var(--ks-spacing-3) var(--ks-spacing-3) 0;
}

.jdbc-modal__title {
    margin: 0 0 var(--ks-spacing-2);
    font-size: var(--ks-font-size-xs);
    font-weight: 600;
    color: var(--ks-text-secondary);
}

.jdbc-modal__empty {
    margin: var(--ks-spacing-2) 0;
    color: var(--ks-text-secondary);
}

.jdbc-modal__table {
    width: 100%;
    border-collapse: collapse;
}

.jdbc-modal__table td {
    padding: var(--ks-spacing-1) var(--ks-spacing-2);
    border-bottom: 1px solid var(--ks-border-primary);
}

.jdbc-modal__code {
    max-height: 20rem;
    overflow: auto;
    padding: var(--ks-spacing-2);
    border: 1px solid var(--ks-border-primary);
    border-radius: var(--ks-border-radius);
    white-space: pre-wrap;
}

.jdbc-modal__files {
    margin: 0;
    padding-left: var(--ks-spacing-4);
}
</style>
