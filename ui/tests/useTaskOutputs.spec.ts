import { describe, expect, it, vi } from "vitest";
import { nextTick } from "vue";
import { useTaskOutputs } from "../src/lib/useTaskOutputs";

const execution = (taskRun: Record<string, any>) => ({ id: "E1", taskRunList: [{ taskId: "q", ...taskRun }] });

describe("useTaskOutputs", () => {
    it("does not call the host without a task run id", async () => {
        const fetchOutputs = vi.fn();
        const { outputs } = useTaskOutputs({ task: { id: "q" }, execution: execution({}), fetchOutputs });
        await nextTick();

        expect(fetchOutputs).not.toHaveBeenCalled();
        expect(outputs.value).toBeNull();
    });

    it("ignores an empty result so the task run outputs win", async () => {
        const fetchOutputs = vi.fn().mockResolvedValue({});
        const { outputs } = useTaskOutputs({
            task: { id: "q" },
            execution: execution({ id: "R1", outputs: { size: 3 } }),
            fetchOutputs,
        });
        await vi.waitFor(() => expect(fetchOutputs).toHaveBeenCalledWith({ taskRunId: "R1" }));
        await nextTick();

        expect(outputs.value).toEqual({ size: 3 });
    });

    it("prefers the fetched outputs", async () => {
        const fetchOutputs = vi.fn().mockResolvedValue({ size: 5 });
        const { outputs } = useTaskOutputs({ task: { id: "q" }, execution: execution({ id: "R1" }), fetchOutputs });
        await vi.waitFor(() => expect(outputs.value).toEqual({ size: 5 }));
    });
});
