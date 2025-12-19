package io.kestra.plugin.jdbc.db2;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
class RunnerTest {

    @Test
    @ExecuteFlow(value = "sanity-checks/all_db2.yaml", timeout = "PT600S")
    @Disabled("""
            Because the runner is full:
            Could not pull image: write /var/lib/docker/tmp/GetImageBlob1585115176: no space left on device
        """)
    void all_db2(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(11));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
