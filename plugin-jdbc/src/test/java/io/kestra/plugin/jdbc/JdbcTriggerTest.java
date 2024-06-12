package io.kestra.plugin.jdbc;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.micronaut.context.ApplicationContext;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class JdbcTriggerTest {
    @Inject
    protected ApplicationContext applicationContext;

    @Test
    void noNpeOnNullSizeQueryOutput() throws Exception {
        Optional<Execution> evaluate = new DoNothingTrigger().evaluate(
            ConditionContext.builder().runContext(
                new RunContext(applicationContext, Collections.emptyMap())
            ).build(),
            new TriggerContext()
        );

        assertThat(evaluate.isEmpty(), is(true));
    }

    @NoArgsConstructor
    public static class DoNothingTrigger extends AbstractJdbcTrigger {
        @Override
        public void registerDriver() {}

        @Override
        protected AbstractJdbcQuery.Output runQuery(RunContext runContext) {
            return AbstractJdbcQuery.Output.builder().build();
        }
    }
}
