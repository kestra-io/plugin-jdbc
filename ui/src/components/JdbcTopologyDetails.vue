<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { computed } from "vue";
import { useI18n } from "vue-i18n";
import { KsTopologyDetails } from "@kestra-io/design-system";
import { summaryRows } from "../lib/details";
import { useTaskOutputs } from "../lib/useTaskOutputs";

const { t } = useI18n({
    inheritLocale: true,
    useScope: "local",
    messages: {
        en: {
            database: "Database",
            fetchType: "Fetch type",
            statements: "Statements",
            rowsAffected: "Rows affected",
            rowsFetched: "Rows fetched",
            duration: "Duration",
        },
    },
});

const props = defineProps<KnownSlotProps["topology-details"]>();

const hasExecution = computed(() => !!props.execution?.id);
const { outputs } = useTaskOutputs(props);

const rows = computed(() =>
    summaryRows({
        task: props.task as Record<string, any>,
        taskType: props.taskType,
        outputs: outputs.value,
        hasExecution: hasExecution.value,
        labels: {
            database: t("database"),
            fetchType: t("fetchType"),
            statements: t("statements"),
            rowsAffected: t("rowsAffected"),
            rowsFetched: t("rowsFetched"),
            duration: t("duration"),
        },
    }),
);
</script>

<template>
    <KsTopologyDetails :rows="rows" />
</template>
