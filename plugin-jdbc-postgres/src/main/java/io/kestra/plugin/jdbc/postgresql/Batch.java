package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
    title = "Execute a batch query to a PostgreSQL server."
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch rows from a table, and bulk insert them to another one.",
            full = true,
            code = """
                id: postgres_bulk_insert
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.postgresql.Query
                    url: jdbc:postgresql://dev:56982/
                    username: pg_user
                    password: pg_password
                    sql: |
                      SELECT *
                      FROM xref
                      LIMIT 1500;
                    fetchType: STORE

                  - id: update
                    type: io.kestra.plugin.jdbc.postgresql.Batch
                    from: "{{ outputs.query.uri }}"
                    url: jdbc:postgresql://prod:56982/
                    username: pg_user
                    password: pg_password
                    sql: |
                      insert into xref values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
                """
        ),

        @Example(
            title = "Fetch rows from a table, and bulk insert them to another one, without using sql query.",
            full = true,
            code = """
                id: postgres_bulk_insert
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.postgresql.Query
                    url: jdbc:postgresql://dev:56982/
                    username: pg_user
                    password: pg_password
                    sql: |
                      SELECT *
                      FROM xref
                      LIMIT 1500;
                    fetchType: STORE

                  - id: update
                    type: io.kestra.plugin.jdbc.postgresql.Batch
                    from: "{{ outputs.query.uri }}"
                    url: jdbc:postgresql://prod:56982/
                    username: pg_user
                    password: pg_password
                    table: xre
                """
        )
    }
)
public class Batch extends AbstractJdbcBatch implements RunnableTask<AbstractJdbcBatch.Output>, PostgresConnectionInterface{
    @Builder.Default
    protected Property<Boolean> ssl = Property.of(false);
    protected Property<SslMode> sslMode;
    protected Property<String> sslRootCert;
    protected Property<String> sslCert;
    protected Property<String> sslKey;
    protected Property<String> sslKeyPassword;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new PostgresCellConverter(zoneId);
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties properties = super.connectionProperties(runContext);
        PostgresService.handleSsl(properties, runContext, this);

        return properties;
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new org.postgresql.Driver());
    }
}
