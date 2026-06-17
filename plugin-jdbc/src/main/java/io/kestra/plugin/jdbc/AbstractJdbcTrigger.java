package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.PebbleUtil;
import io.swagger.v3.oas.annotations.media.Schema;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.kestra.core.models.enums.MonacoLanguages;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<AbstractJdbcQuery.Output>, JdbcQueryInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @PluginProperty(group = "connection")
    private Property<String> url;

    @PluginProperty(group = "connection")
    private Property<String> username;

    @PluginProperty(group = "connection")
    private Property<String> password;

    @PluginProperty(group = "advanced")
    private Property<String> timeZoneId;

    @NotNull
    @PluginProperty(language = MonacoLanguages.SQL, group = "main")
    private Property<String> sql;

    @PluginProperty(language = MonacoLanguages.SQL, group = "advanced")
    private Property<String> afterSQL;

    /**
     * @deprecated use fetchType: STORE instead
     */
    @Builder.Default
    @Deprecated(since="0.19.0", forRemoval=true)
    private boolean store = false;

    /**
     * @deprecated use fetchType: FETCH_ONE instead
     */
    @Builder.Default
    @Deprecated(since="0.19.0", forRemoval=true)
    private boolean fetchOne = false;

    /**
     * @deprecated use fetchType: FETCH instead
     */
    @Builder.Default
    @Deprecated(since="0.19.0", forRemoval=true)
    private boolean fetch = false;

    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    @Schema(
        title = "The way you want to fetch the data.",
        description = """
            Triggers default to `FETCH`, which loads all rows into memory and exposes them as `{{ trigger.rows }}`. \
            A trigger fires only when the query returns at least one row; setting `NONE` would cause the trigger to never fire. \
            Use `FETCH_ONE` to expose a single row as `{{ trigger.row }}`, or `STORE` to write the rows to internal storage and expose the file URI as `{{ trigger.uri }}`."""
    )
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @AssertTrue(message = "fetchType NONE is not valid for triggers — the trigger would never fire. Use FETCH, FETCH_ONE, or STORE.")
    @JsonIgnore
    boolean isFetchTypeValid() {
        if (fetchType == null) return true;
        var expr = fetchType.toString();
        // Skip validation for dynamic Pebble expressions — those can only be checked at render time
        if (PebbleUtil.containsOpeningBlockDelimiter(expr)) return true;
        return !FetchType.NONE.name().equalsIgnoreCase(expr);
    }

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

        String rSql = runContext.render(this.sql).as(String.class).orElseThrow();

        var run = runQuery(runContext);

        logger.debug("Found '{}' rows from '{}'", run.getSize(), rSql);

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
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElseThrow();
        if (rFetchType == FetchType.NONE) {
            throw new IllegalArgumentException("fetchType NONE is not valid for triggers — the trigger would never fire. Use FETCH, FETCH_ONE, or STORE.");
        }
        return rFetchType;
    }

    protected abstract AbstractJdbcQuery.Output runQuery(RunContext runContext) throws Exception;
}
