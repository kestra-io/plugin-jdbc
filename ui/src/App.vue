<script setup lang="ts">
import JdbcTopologyDetails from "./components/JdbcTopologyDetails.vue";
import JdbcTaskModal from "./components/JdbcTaskModal.vue";
import JdbcExplorer from "./components/JdbcExplorer.vue";
import { SLOTS } from "@kestra-io/artifact-sdk";
</script>

<template>
    <div style="padding: 1rem; font-family: sans-serif">
        <JdbcTopologyDetails v-bind="SLOTS['topology-details'].defaultProps" />
        <JdbcTaskModal v-bind="SLOTS['topology-task-modal'].defaultProps" />
        <JdbcExplorer v-bind="{ ...SLOTS['topology-task-modal'].defaultProps, taskType: 'io.kestra.plugin.jdbc.duckdb.Query' }" />
    </div>
</template>
