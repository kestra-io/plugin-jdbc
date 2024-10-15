package io.kestra.plugin.jdbc;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
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

    private String url;

    private String username;

    private String password;

    private String timeZoneId;

    private String sql;

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
    protected FetchType fetchType = FetchType.NONE;

    @Builder.Default
    protected Integer fetchSize = 10000;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Object> additionalVars = new HashMap<>();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        var run = runQuery(runContext);

        logger.debug("Found '{}' rows from '{}'", run.getSize(), runContext.render(this.sql));

        if (Optional.ofNullable(run.getSize()).orElse(0L) == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }

    @Override
    public FetchType getFetchType() {
        if(this.fetch) {
            return FetchType.FETCH;
        } else if(this.fetchOne) {
            return FetchType.FETCH_ONE;
        } else if(this.store) {
            return FetchType.STORE;
        }
        return fetchType;
    }

    protected abstract AbstractJdbcQuery.Output runQuery(RunContext runContext) throws Exception;
}
