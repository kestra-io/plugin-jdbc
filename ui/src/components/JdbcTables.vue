<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { computed } from "vue";
import { useI18n } from "vue-i18n";
import { DASH } from "../lib/details";
import { useTaskOutputs } from "../lib/useTaskOutputs";
import "./jdbc-details.css";

const { t } = useI18n({
    inheritLocale: true,
    useScope: "local",
    messages: {
        en: {
            table: "Table",
            access: "Access",
            tables: "Tables",
            tablesNote: "Detected from the SQL text, not a full parser. CTEs and dialect-specific syntax can be missed.",
            tablesTruncated: "The list is truncated to 50 tables.",
            noTables: "No table detected.",
            noExecution: "No execution yet.",
        },
    },
});

defineOptions({ inheritAttrs: false });
const props = defineProps<KnownSlotProps["execution-artifact"]>();
const { outputs: data } = useTaskOutputs(props);

const accessClass = (access?: string) =>
    access === "READ" ? "ok" : access === "WRITE" ? "info" : access ? "warn" : "";
const tables = computed(() => (Array.isArray(data.value?.tables) ? (data.value!.tables as any[]) : []));
const tableName = (table: any) =>
    [table.database, table.schema, table.name].filter((part) => typeof part === "string").join(".");
</script>

<template>
    <div class="jdbc-modal">
        <p v-if="!data" class="jdbc-modal__empty">{{ t("noExecution") }}</p>
        <section v-else class="jdbc-modal__section">
            <h4 class="jdbc-modal__title">{{ t("tables") }}</h4>
            <p v-if="tables.length === 0" class="jdbc-modal__empty">{{ t("noTables") }}</p>
            <div v-else class="jdbc-modal__scroll">
                <table class="jdbc-modal__table">
                    <thead>
                        <tr>
                            <th>{{ t("table") }}</th>
                            <th>{{ t("access") }}</th>
                            <th v-if="tables.some((table) => table.kind)" />
                        </tr>
                    </thead>
                    <tbody>
                        <tr v-for="(table, index) in tables" :key="index">
                            <td class="jdbc-modal__mono">{{ tableName(table) }}</td>
                            <td><span class="jdbc-modal__pill" :class="accessClass(table.access)">{{ table.access ?? DASH }}</span></td>
                            <td v-if="tables.some((other) => other.kind)" class="jdbc-modal__dim">{{ table.kind ?? "" }}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
            <p v-if="tables.length" class="jdbc-modal__note">{{ t("tablesNote") }}</p>
            <p v-if="data.tablesTruncated" class="jdbc-modal__empty">{{ t("tablesTruncated") }}</p>
        </section>
    </div>
</template>
