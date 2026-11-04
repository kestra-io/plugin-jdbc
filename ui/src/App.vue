<script setup lang="ts">
import JdbcTaskModal from "./components/JdbcTaskModal.vue";
import { SLOTS } from "@kestra-io/artifact-sdk";
</script>

<template>
    <div style="padding: 1rem; font-family: sans-serif">
        <JdbcTaskModal v-bind="SLOTS['execution-artifact'].defaultProps" />
    </div>
</template>
