package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<AbstractJdbcQuery.Output>, JdbcQueryInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private Property<String> url;

    private Property<String> username;

    private Property<String> password;

    private Property<String> timeZoneId;

    private Property<String> sql;

    @Builder.Default
    @Deprecated(since="0.19.0", forRemoval=true)
    private boolean store = false;

    @Builder.Default
    @Deprecated(since="0.19.0", forRemoval=true)
    private boolean fetchOne = false;

    @Builder.Default
    @Deprecated(since="0.19.0", forRemoval=true)
    private boolean fetch = false;

    @NotNull
    @Builder.Default
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.NONE);

    @Builder.Default
    protected Property<Integer> fetchSize = Property.ofValue(10000);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Object> additionalVars = new HashMap<>();

    protected Property<Map<String, Object>> parameters;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        var run = runQuery(runContext);

        logger.debug("Found '{}' rows from '{}'", run.getSize(), runContext.render(this.sql).as(String.class).orElse(null));

        if (Optional.ofNullable(run.getSize()).orElse(0L) == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }

    public FetchType renderFetchType(RunContext runContext) throws IllegalVariableEvaluationException {
        if(this.fetch) {
            return FetchType.FETCH;
        } else if(this.fetchOne) {
            return FetchType.FETCH_ONE;
        } else if(this.store) {
            return FetchType.STORE;
        }
        return runContext.render(fetchType).as(FetchType.class).orElseThrow();
    }

    protected abstract AbstractJdbcQuery.Output runQuery(RunContext runContext) throws Exception;
}
