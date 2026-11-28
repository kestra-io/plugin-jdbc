package io.kestra.plugin.jdbc.openedge;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute multiple SQL statements against Progress OpenEdge",
    description = "Runs multiple SQL statements against an OpenEdge database through the Progress DataDirect JDBC driver. The proprietary driver must be supplied separately."
)
@Plugin(
    examples = {
        @Example(
            title = "Run multiple SQL statements against OpenEdge.",
            full = true,
            code = """
                   id: openedge_queries
                   namespace: company.team

                   tasks:
                     - id: queries
                       type: io.kestra.plugin.jdbc.openedge.Queries
                       url: jdbc:datadirect:openedge://erp-db.example.com:20000;databaseName=erp
                       username: "{{ secret('OPENEDGE_USERNAME') }}"
                       password: "{{ secret('OPENEDGE_PASSWORD') }}"
                       sql: SELECT COUNT(*) FROM PUB.customer; SELECT COUNT(*) FROM PUB.orders
                       fetchType: FETCH
                   """
        )
    },
    metrics = {
        @Metric(name = "fetch.size", type = Counter.TYPE, unit = "rows", description = "The number of fetched rows."),
        @Metric(name = "duration", type = Timer.TYPE, description = "The duration of SQL execution, including reading result rows."),
        @Metric(name = "rows.affected", type = Counter.TYPE, unit = "rows", description = "The number of rows affected, as reported by the JDBC driver."),
        @Metric(name = "bytes", type = Counter.TYPE, unit = "bytes", description = "The size of the stored result file.")
    }
)
public class Queries extends AbstractJdbcQueries implements OpenEdgeConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new OpenEdgeCellConverter(zoneId);
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
