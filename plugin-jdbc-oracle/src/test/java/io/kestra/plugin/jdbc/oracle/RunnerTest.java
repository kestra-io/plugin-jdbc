package io.kestra.plugin.jdbc.oracle;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
class RunnerTest {

    @Test
    @ExecuteFlow("sanity-checks/all_oracle.yaml")
    void all_oracle(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(12));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
