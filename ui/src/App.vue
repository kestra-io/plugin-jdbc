<script setup lang="ts">
import JdbcTopologyDetails from "./components/JdbcTopologyDetails.vue";
import JdbcTaskModal from "./components/JdbcTaskModal.vue";
import { SLOTS } from "@kestra-io/artifact-sdk";
</script>

<template>
    <div style="padding: 1rem; font-family: sans-serif">
        <JdbcTopologyDetails v-bind="SLOTS['topology-details'].defaultProps" />
        <JdbcTaskModal v-bind="SLOTS['topology-task-modal'].defaultProps" />
    </div>
</template>
