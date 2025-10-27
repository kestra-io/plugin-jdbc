package io.kestra.plugin.jdbc.mysql;

import com.mysql.cj.jdbc.Driver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.micronaut.http.uri.UriBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a MySQL batch-query."
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch rows from a table, and bulk insert them to another one.",
            full = true,
            code = """
                id: mysql_batch
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.mysql.Query
                    url: jdbc:mysql://127.0.0.1:3306/
                    username: mysql_user
                    password: mysql_password
                    sql: |
                      SELECT *
                      FROM xref
                      LIMIT 1500;
                    fetchType: STORE

                  - id: update
                    type: io.kestra.plugin.jdbc.mysql.Batch
                    from: "{{ outputs.query.uri }}"
                    url: jdbc:mysql://127.0.0.1:3306/
                    username: mysql_user
                    password: mysql_password
                    sql: |
                      insert into xref values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
                """
        ),
        @Example(
            title = "Fetch rows from a table, and bulk insert them to another one, without using sql query.",
            full = true,
            code = """
                id: mysql_batch
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.mysql.Query
                    url: jdbc:mysql://127.0.0.1:3306/
                    username: mysql_user
                    password: mysql_password
                    sql: |
                      SELECT *
                      FROM xref
                      LIMIT 1500;
                    fetchType: STORE

                  - id: update
                    type: io.kestra.plugin.jdbc.mysql.Batch
                    from: "{{ outputs.query.uri }}"
                    url: jdbc:mysql://127.0.0.1:3306/
                    username: mysql_user
                    password: mysql_password
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
public class Batch extends AbstractJdbcBatch implements MySqlConnectionInterface {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new MysqlCellConverter(zoneId);
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties props = super.connectionProperties(runContext);

        URI url = URI.create((String) props.get("jdbc.url"));
        url = URI.create(url.getSchemeSpecificPart());

        UriBuilder builder = UriBuilder.of(url);

        builder.scheme("jdbc:mysql");

        props.put("generateSimpleParameterMetadata", "true");

        props.put("jdbc.url", builder.build().toString());

        return props;
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }
}
