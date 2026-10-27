<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { computed, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { KsTopologyDetails } from "@kestra-io/design-system";
import { summaryRows } from "../lib/details";

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

// Outputs are only complete once the task run ends, so a state change reloads them too.
watch(
    () => [props.execution?.id, taskRun.value?.state?.current],
    ([id]) => {
        if (id) loadOutputs();
    },
    { immediate: true },
);

const rows = computed(() =>
    summaryRows({
        task: props.task as Record<string, any>,
        taskType: props.taskType,
        outputs: outputs.value ?? taskRun.value?.outputs ?? null,
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
