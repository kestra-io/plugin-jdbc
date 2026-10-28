<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { useI18n } from "vue-i18n";
import { useTaskOutputs } from "../lib/useTaskOutputs";
import "./jdbc-details.css";

const { t } = useI18n({
    inheritLocale: true,
    useScope: "local",
    messages: {
        en: {
            statement: "Statement",
            statementNote: "Secret values appear as ***. Truncated at 64 KiB.",
            noStatement: "No statement stored. Set outputStatement to true to keep it.",
        },
    },
});

defineOptions({ inheritAttrs: false });
const props = defineProps<KnownSlotProps["execution-artifact"]>();
const { outputs: data } = useTaskOutputs(props);
</script>

<template>
    <div class="jdbc-modal">
        <section class="jdbc-modal__section">
            <h4 class="jdbc-modal__title">{{ t("statement") }}</h4>
            <template v-if="typeof data?.statement === 'string'">
                <pre class="jdbc-modal__code">{{ data.statement }}</pre>
                <p class="jdbc-modal__note">{{ t("statementNote") }}</p>
            </template>
            <p v-else class="jdbc-modal__empty">{{ t("noStatement") }}</p>
        </section>
    </div>
</template>
