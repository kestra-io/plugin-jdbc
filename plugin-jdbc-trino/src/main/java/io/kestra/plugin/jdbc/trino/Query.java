package io.kestra.plugin.jdbc.trino;

import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.executions.metrics.Counter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.trino.jdbc.TrinoDriver;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a data lake using Trino query engine.",
    description = "Make sure NOT to include semicolon at the end of your SQL query. Adding semicolon at the end will result in an error. If you want to test this integration, search for Trino in Blueprints - you'll find detailed instructions there."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Generate a CSV file report from a SQL query using Trino",
            code = """
                   id: trino_query
                   namespace: company.team

                   tasks:
                     - id: analyze_orders
                       type: io.kestra.plugin.jdbc.trino.Query
                       url: jdbc:trino://localhost:8080/tpch
                       username: "{{ secret('TRINO_USERNAME') }}"
                       password: "{{ secret('TRINO_PASSWORD') }}"
                       sql: |
                         select orderpriority as priority, sum(totalprice) as total
                         from tpch.tiny.orders
                         group by orderpriority
                         order by orderpriority
                       fetchType: FETCH
                       fetchType: STORE

                     - id: csv_report
                       type: io.kestra.plugin.serdes.csv.IonToCsv
                       from: "{{ outputs.analyze_orders.uri }}"
                   """
        )
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of fetched rows."
        )
    }
)
public class Query extends AbstractJdbcQuery implements TrinoConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new TrinoCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(TrinoDriver.class::isInstance)) {
            DriverManager.registerDriver(new TrinoDriver());
        }
    }

}
