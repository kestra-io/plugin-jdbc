package io.kestra.plugin.jdbc.vectorwise;

import com.ingres.jdbc.IngresDriver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a Vectorwise database batch-query."
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch rows from a table and bulk insert to another one.",
            full = true,
            code = """
                   id: vectorwise_batch_query
                   namespace: company.team

                   tasks:
                     - id: query
                       type: io.kestra.plugin.jdbc.vectorwise.Query
                       url: jdbc:vectorwise://dev:port/base
                       username: "{{ secret('VECTORWISE_USERNAME') }}"
                       password: "{{ secret('VECTORWISE_PASSWORD') }}"
                       sql: |
                         SELECT *
                         FROM xref
                         LIMIT 1500;
                       fetchType: STORE

                     - id: update
                       type: io.kestra.plugin.jdbc.vectorwise.Batch
                       from: "{{ outputs.query.uri }}"
                       url: jdbc:vectorwise://prod:port/base
                       username: "{{ secret('VECTORWISE_USERNAME') }}"
                       password: "{{ secret('VECTORWISE_PASSWORD') }}"
                       sql: insert into xref values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
                   """
        ),
        @Example(
            title = "Fetch rows from a table and bulk insert to another one without using sql query.",
            full = true,
            code = {
                "id: vectorwise_batch_query",
                "namespace: company.team",
                "",
                "tasks:",
                "  - id: query",
                "    type: io.kestra.plugin.jdbc.vectorwise.Query",
                "    url: jdbc:vectorwise://dev:port/base",
                "    username: admin",
                "    password: admin_passwd",
                "    sql: |",
                "      SELECT *",
                "      FROM xref",
                "      LIMIT 1500;",
                "    fetchType: STORE",
                "",
                "  - id: update",
                "    type: io.kestra.plugin.jdbc.vectorwise.Batch",
                "    from: \"{{ outputs.query.uri }}\"",
                "    url: jdbc:vectorwise://prod:port/base",
                "    username: admin",
                "    password: admin_passwd",
                "    table: xref",
            }
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "records",
            description = "The number of records processed."
        ),
        @Metric(
            name = "updated",
            type = Counter.TYPE,
            unit = "records",
            description = "The number of records updated."
        ),
        @Metric(
            name = "query",
            type = Counter.TYPE,
            unit = "queries",
            description = "The number of batch queries executed."
        )
    }
)
public class Batch extends AbstractJdbcBatch implements VetorwiseConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new VectorwiseCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(IngresDriver.class::isInstance)) {
            DriverManager.registerDriver(new IngresDriver());
        }
    }
}


