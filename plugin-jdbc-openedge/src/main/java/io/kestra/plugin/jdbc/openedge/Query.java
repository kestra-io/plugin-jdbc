package io.kestra.plugin.jdbc.openedge;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
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
    title = "Execute a single SQL query against Progress OpenEdge",
    description = "Runs one SQL statement against an OpenEdge database through the Progress DataDirect JDBC driver. Supports FETCH, FETCH_ONE and STORE result modes. The proprietary driver must be supplied separately."
)
@Plugin(
    examples = {
        @Example(
            title = "Query an OpenEdge table and store the result.",
            full = true,
            code = """
                   id: openedge_query
                   namespace: company.team

                   tasks:
                     - id: query
                       type: io.kestra.plugin.jdbc.openedge.Query
                       url: jdbc:datadirect:openedge://erp-db.example.com:20000;databaseName=erp
                       username: "{{ secret('OPENEDGE_USERNAME') }}"
                       password: "{{ secret('OPENEDGE_PASSWORD') }}"
                       sql: SELECT TOP 100 * FROM PUB.customer
                       fetchType: STORE
                   """
        )
    },
    metrics = {
        @Metric(name = "fetch.size", type = Counter.TYPE, unit = "rows", description = "The number of fetched rows."),
        @Metric(name = "duration", type = Timer.TYPE, description = "The duration of the SQL execution, including reading the result rows."),
        @Metric(name = "rows.affected", type = Counter.TYPE, unit = "rows", description = "The number of rows affected, as reported by the JDBC driver."),
        @Metric(name = "bytes", type = Counter.TYPE, unit = "bytes", description = "The size of the stored result file.")
    }
)
public class Query extends AbstractJdbcQuery implements OpenEdgeConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new OpenEdgeCellConverter(zoneId);
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
