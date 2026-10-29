import { computed, ref, watch } from "vue";

type SlotProps = {
    task?: { id?: string } | Record<string, any>;
    execution?: { id?: string; taskRunList?: any[] };
    fetchOutputs?: (opts: { taskRunId?: string }) => Promise<Record<string, any> | undefined | null>;
};

export function useTaskOutputs(props: SlotProps) {
    const taskRun = computed(() => props.execution?.taskRunList?.filter((tr: any) => tr.taskId === props.task?.id).at(-1));
    const fetched = ref<Record<string, any> | null>(null);

    async function load() {
        // The host resolves `{}` without a task run id, which would shadow the outputs of the task run itself.
        const taskRunId = taskRun.value?.id;
        if (!taskRunId) return;
        try {
            const result = await props.fetchOutputs?.({ taskRunId });
            // A slow response for a previous task run must not overwrite the current one.
            if (taskRun.value?.id !== taskRunId) return;
            if (result && Object.keys(result).length > 0) fetched.value = result;
        } catch {
            /* best-effort */
        }
    }

    // A retry or another execution must not keep showing the previous task run outputs.
    watch(() => taskRun.value?.id, () => {
        fetched.value = null;
    });

    // Outputs are only complete once the task run ends, so a state change reloads them too.
    watch(() => [taskRun.value?.id, taskRun.value?.state?.current], load, { immediate: true });

    const outputs = computed(() => fetched.value ?? taskRun.value?.outputs ?? null);
    return { taskRun, outputs };
}
