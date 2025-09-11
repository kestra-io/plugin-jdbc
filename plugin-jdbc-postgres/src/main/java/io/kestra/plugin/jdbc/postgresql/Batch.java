package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.postgresql.Driver;

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
    title = "Run a PostgreSQL batch-query."
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
                    url: jdbc:postgresql://dev:5432/
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    sql: |
                      SELECT *
                      FROM xref
                      LIMIT 1500;
                    fetchType: STORE

                  - id: update
                    type: io.kestra.plugin.jdbc.postgresql.Batch
                    from: "{{ outputs.query.uri }}"
                    url: jdbc:postgresql://prod:5433/
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
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
                    url: jdbc:postgresql://dev:5432/
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    sql: |
                      SELECT *
                      FROM xref
                      LIMIT 1500;
                    fetchType: STORE

                  - id: update
                    type: io.kestra.plugin.jdbc.postgresql.Batch
                    from: "{{ outputs.query.uri }}"
                    url: jdbc:postgresql://prod:5433/
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    table: xre
                """
        ),
        @Example(
            full = true,
            title = "Use Postgres Batch to bulk insert rows",
            code = """
                id: postgres_batch
                namespace: company.team

                tasks:
                  - id: download_products_csv_file
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/products.csv

                  - id: products_csv_to_ion
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ outputs.download_products_csv_file.uri }}"

                  - id: postgres_create_table
                    type: io.kestra.plugin.jdbc.postgresql.Query
                    url: "jdbc:postgresql://{{ secret('POSTGRES_HOST') }}:5432/postgres"
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    sql: |
                      CREATE TABLE IF NOT EXISTS products(
                        product_id varchar(5),
                        product_name varchar(100),
                        product_category varchar(50),
                        brand varchar(50)
                      )

                  - id: postgres_batch_insert
                    type: io.kestra.plugin.jdbc.postgresql.Batch
                    url: "jdbc:postgresql://{{ secret('POSTGRES_HOST') }}:5432/postgres"
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    from: "{{ outputs.products_csv_to_ion.uri }}"
                    sql: |
                      insert into products values (?, ?, ?, ?)
              """
        )
    }
)
public class Batch extends AbstractJdbcBatch implements RunnableTask<AbstractJdbcBatch.Output>, PostgresConnectionInterface{
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<Boolean> ssl = Property.ofValue(false);
    @PluginProperty(group = "connection")
    protected Property<SslMode> sslMode;
    @PluginProperty(group = "connection")
    protected Property<String> sslRootCert;
    @PluginProperty(group = "connection")
    protected Property<String> sslCert;
    @PluginProperty(group = "connection")
    protected Property<String> sslKey;
    @PluginProperty(group = "connection")
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
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }
}
