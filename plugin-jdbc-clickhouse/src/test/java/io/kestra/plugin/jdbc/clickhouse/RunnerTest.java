package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.State;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
class RunnerTest {

    @Test
    @ExecuteFlow(value = "sanity-checks/all_clickhouse.yaml", timeout = "PT600S")
    void all_clickhouse(Execution execution) {
        // the readiness task retries until the container is up, which can duplicate its task run,
        // so assert on distinct task ids rather than the raw task run count
        assertThat(execution.getTaskRunList().stream().map(TaskRun::getTaskId).distinct().toList(), hasSize(13));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
