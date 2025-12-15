package io.kestra.plugin.jdbc;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class JdbcTriggerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void noNpeOnNullSizeQueryOutput() throws Exception {
        DoNothingTrigger trigger = DoNothingTrigger.builder()
            .sql(Property.ofValue("SELECT 1"))
            .build();

        Optional<Execution> evaluate = trigger.evaluate(
            ConditionContext.builder()
                .runContext(runContextFactory.of())
                .build(),
            new TriggerContext()
        );

        assertThat(evaluate.isEmpty(), is(true));
    }

    @NoArgsConstructor
    @SuperBuilder
    public static class DoNothingTrigger extends AbstractJdbcTrigger {

        @Override
        public void registerDriver() {
        }

        @Override
        public String getScheme() {
            return null;
        }

        @Override
        protected AbstractJdbcQuery.Output runQuery(RunContext runContext) {
            return AbstractJdbcQuery.Output.builder().build();
        }
    }
}
