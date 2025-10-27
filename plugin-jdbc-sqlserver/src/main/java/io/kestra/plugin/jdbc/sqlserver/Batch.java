package io.kestra.plugin.jdbc.sqlserver;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
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
    title = "Run a Microsoft SQL Server batch-query."
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch rows from a table and bulk insert to another one.",
            full = true,
            code = """
                id: sqlserver_batch_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.sqlserver.Query
                    url: jdbc:sqlserver://dev:41433;trustServerCertificate=true
                    username: "{{ secret('SQL_USERNAME') }}"
                    password: "{{ secret('SQL_PASSWORD') }}"
                    sql: |
                      SELECT *
                      FROM xref
                      LIMIT 1500;
                    fetchType: STORE

                  - id: update
                    type: io.kestra.plugin.jdbc.sqlserver.Batch
                    from: "{{ outputs.query.uri }}"
                    url: jdbc:sqlserver://prod:41433;trustServerCertificate=true
                    username: "{{ secret('SQL_USERNAME') }}"
                    password: "{{ secret('SQL_PASSWORD') }}"
                    sql: |
                      insert into xref values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
                """
        ),
        @Example(
            title = "Fetch rows from a table and bulk insert to another one, without using sql query.",
            full = true,
            code = """
                id: sqlserver_batch_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.sqlserver.Query
                    url: jdbc:sqlserver://dev:41433;trustServerCertificate=true
                    username: sql_server_user
                    password: sql_server_passwd
                    sql: |
                      SELECT *
                      FROM xref
                      LIMIT 1500;
                    fetchType: STORE

                  - id: update",
                    type: io.kestra.plugin.jdbc.sqlserver.Batch
                    from: "{{ outputs.query.uri }}"
                    url: jdbc:sqlserver://prod:41433;trustServerCertificate=true
                    username: "{{ secret('SQL_USERNAME') }}"
                    password: "{{ secret('SQL_PASSWORD') }}"
                    table: xref
              """
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
public class Batch extends AbstractJdbcBatch implements SqlServerConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SqlServerCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(SQLServerDriver.class::isInstance)) {
            DriverManager.registerDriver(new SQLServerDriver());
        }
    }
}
