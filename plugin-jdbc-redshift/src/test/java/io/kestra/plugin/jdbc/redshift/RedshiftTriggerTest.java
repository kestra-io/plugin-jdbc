package io.kestra.plugin.jdbc.redshift;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.TriggerState;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class RedshiftTriggerTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void noRowsDoesNotTrigger() throws Exception {
        Trigger trigger = new Trigger(0L);

        Map.Entry<ConditionContext, TriggerState> context =
            TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> execution = trigger.evaluate(
            context.getKey(),
            context.getValue().context()
        );

        assertThat(execution.isEmpty(), is(true));
    }

    @Test
    void rowsTriggerExecution() throws Exception {
        Trigger trigger = new Trigger(1L);

        Map.Entry<ConditionContext, TriggerState> context =
            TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> execution = trigger.evaluate(
            context.getKey(),
            context.getValue().context()
        );

        assertThat(execution.isPresent(), is(true));
    }

    static class Trigger extends io.kestra.plugin.jdbc.redshift.Trigger {
        private final Long resultSize;

        Trigger(Long resultSize) {
            this.resultSize = resultSize;
        }

        @Override
        protected AbstractJdbcQuery.Output runQuery(RunContext runContext) {
            return AbstractJdbcQuery.Output.builder()
                .size(resultSize)
                .build();
        }
    }
}
