package io.kestra.plugin.jdbc;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest
class JdbcTriggerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected Validator validator;

    @Test
    void fetchTypeNoneFailsValidation() {
        var trigger = DoNothingTrigger.builder()
            .id("test")
            .type(DoNothingTrigger.class.getName())
            .sql(Property.ofValue("SELECT 1"))
            .fetchType(Property.ofValue(FetchType.NONE))
            .build();

        Set<ConstraintViolation<DoNothingTrigger>> violations = validator.validate(trigger);

        var fetchTypeViolations = violations.stream()
            .filter(v -> v.getMessage().contains("fetchType NONE"))
            .toList();
        assertThat(fetchTypeViolations, hasSize(1));
        assertThat(fetchTypeViolations.getFirst().getMessage(), is(
            "fetchType NONE is not valid for triggers — the trigger would never fire. Use FETCH, FETCH_ONE, or STORE."
        ));
    }

    @Test
    void fetchTypeDynamicExpressionPassesValidation() {
        var trigger = DoNothingTrigger.builder()
            .id("test")
            .type(DoNothingTrigger.class.getName())
            .sql(Property.ofValue("SELECT 1"))
            .fetchType(Property.ofExpression("{{ inputs.fetchType }}"))
            .build();

        Set<ConstraintViolation<DoNothingTrigger>> violations = validator.validate(trigger);

        assertThat(violations.stream()
            .filter(v -> v.getMessage().contains("fetchType NONE"))
            .toList(), hasSize(0));
    }

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
