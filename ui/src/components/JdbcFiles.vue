<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { computed } from "vue";
import { useI18n } from "vue-i18n";
import { fileHref } from "../lib/details";
import { useTaskOutputs } from "../lib/useTaskOutputs";
import "./jdbc-details.css";

const { t } = useI18n({
    inheritLocale: true,
    useScope: "local",
    messages: {
        en: {
            files: "Files",
            download: "Open",
            results: "Results",
            statementsFile: "Per statement details",
            columnStats: "Column statistics",
            noFiles: "No file stored.",
        },
    },
});

defineOptions({ inheritAttrs: false });
const props = defineProps<KnownSlotProps["execution-artifact"]>();
const { outputs: data } = useTaskOutputs(props);

const tenant = computed(() => props.tenant ?? window.localStorage?.getItem("selectedTenant") ?? "main");
const href = (uri: string) => fileHref(uri, tenant.value, props.execution?.id as string | undefined);

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
        <section class="jdbc-modal__section">
            <h4 class="jdbc-modal__title">{{ t("files") }}</h4>
            <p v-if="!files.length" class="jdbc-modal__empty">{{ t("noFiles") }}</p>
            <div v-for="file in files" :key="file.key" class="jdbc-modal__kv">
                <span>{{ file.label }}</span>
                <a v-if="href(file.uri)" class="jdbc-modal__link" :href="href(file.uri)" target="_blank" rel="noopener noreferrer">{{ t("download") }} ↗</a>
                <code v-else class="jdbc-modal__mono">{{ file.uri }}</code>
            </div>
        </section>
    </div>
</template>
