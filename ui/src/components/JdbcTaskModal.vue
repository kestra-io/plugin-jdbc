<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { computed } from "vue";
import { useI18n } from "vue-i18n";
import { KsTopologyDetails } from "@kestra-io/design-system";
import { formatBytes, formatDuration, formatNumber } from "../lib/details";
import { useTaskOutputs } from "../lib/useTaskOutputs";
import JdbcFiles from "./JdbcFiles.vue";
import JdbcStatement from "./JdbcStatement.vue";
import JdbcTables from "./JdbcTables.vue";
import "./jdbc-details.css";

const { t } = useI18n({
    inheritLocale: true,
    useScope: "local",
    messages: {
        en: {
            execution: "Execution",
            status: "Status",
            rowsAffected: "Rows affected",
            rowsFetched: "Rows fetched",
            storedSize: "Stored size",
            duration: "Duration",
            warnings: "Warnings",
            noExecution: "No execution yet.",
        },
    },
});

// The tables, statement and files sections are also registered on their own as execution artifacts.
const props = defineProps<KnownSlotProps["topology-task-modal"]>();
const { taskRun, outputs: data } = useTaskOutputs(props);

const executionRows = computed(() => {
    const o = data.value ?? {};
    return [
        typeof o.rowsAffected === "number"
            ? { label: t("rowsAffected"), value: formatNumber(o.rowsAffected) }
            : { label: t("rowsFetched"), value: formatNumber(o.size) },
        { label: t("storedSize"), value: formatBytes(o.bytes) },
        { label: t("duration"), value: formatDuration(o.durationMs) },
        { label: t("warnings"), value: formatNumber(o.warnings) },
    ];
});

const state = computed(() => (taskRun.value?.state?.current as string | undefined) ?? undefined);
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
            <JdbcTables v-bind="props" />
            <JdbcStatement v-if="typeof data.statement === 'string'" v-bind="props" />
            <JdbcFiles v-if="['uri', 'statements', 'columnStats'].some((key) => typeof data![key] === 'string')" v-bind="props" />
        </template>
    </div>
</template>
