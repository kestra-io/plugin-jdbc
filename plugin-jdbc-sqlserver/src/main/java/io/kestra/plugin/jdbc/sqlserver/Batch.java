package io.kestra.plugin.jdbc.sqlserver;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
    title = "Bulk insert rows into Microsoft SQL Server using prepared statements",
    description = "Reads ION-formatted data from Kestra internal storage and performs high-performance batch inserts using JDBC batch operations. Data is processed in chunks (default 1,000 rows) to optimize memory and performance. Supports auto-commit for databases without transaction support."
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
                      SELECT TOP (1500) *
                      FROM xref;
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
                      SELECT TOP (1500) *
                      FROM xref;
                    fetchType: STORE

                  - id: update
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
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<EncryptMode> encrypt = Property.ofValue(EncryptMode.FALSE);
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<Boolean> trustServerCertificate = Property.ofValue(false);
    @PluginProperty(group = "connection")
    protected Property<String> hostNameInCertificate;
    @PluginProperty(group = "connection")
    protected Property<String> trustStore;
    @PluginProperty(group = "connection")
    protected Property<String> trustStorePassword;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SqlServerCellConverter(zoneId);
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        var properties = super.connectionProperties(runContext);
        SqlServerService.handleSsl(properties, runContext, this);
        return properties;
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(SQLServerDriver.class::isInstance)) {
            DriverManager.registerDriver(new SQLServerDriver());
        }
    }
}
