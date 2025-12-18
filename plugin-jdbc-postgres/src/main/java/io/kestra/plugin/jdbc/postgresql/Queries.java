package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
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
    title = "Run multiple PostgreSQL queries."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results in a task.",
            code = """
                id: postgres_query
                namespace: company.team

                tasks:
                  - id: fetch
                    type: io.kestra.plugin.jdbc.postgresql.Queries
                    url: jdbc:postgresql://127.0.0.1:56982/
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    sql: |
                      SELECT firstName, lastName FROM employee;
                      SELECT brand FROM laptop;
                    fetchType: FETCH
                """
        ),
        @Example(
            full = true,
            title = "Use Postgres Queries to run multiple queries",
            code = """
                id: postgres_queries
                namespace: company.team

                tasks:
                  - id: init_products
                    type: io.kestra.plugin.jdbc.postgresql.Queries
                    url: "jdbc:postgresql://{{secret('POSTGRES_HOST')}}:5432/postgres"
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    sql: |
                      CREATE TABLE IF NOT EXISTS products(
                        product_id SERIAL PRIMARY KEY,
                        product_name varchar(100),
                        product_category varchar(50),
                        brand varchar(50)
                      );
                      INSERT INTO products VALUES(1, 'streamline turn-key systems','Electronics','gomez') ON CONFLICT (product_id) DO NOTHING;
                      INSERT INTO products VALUES(2, 'morph viral applications','Household','wolfe') ON CONFLICT (product_id) DO NOTHING;
                      INSERT INTO products VALUES(3, 'expedite front-end schemas','Household','davis-martinez') ON CONFLICT (product_id) DO NOTHING;
                      INSERT INTO products VALUES(4, 'syndicate robust ROI','Outdoor','ruiz-price') ON CONFLICT (product_id) DO NOTHING;
                    fetchType: NONE
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
public class Queries extends AbstractJdbcQueries implements PostgresConnectionInterface {
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
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties properties = super.connectionProperties(runContext);
        PostgresService.handleSsl(properties, runContext, this);

        return properties;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new PostgresCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
