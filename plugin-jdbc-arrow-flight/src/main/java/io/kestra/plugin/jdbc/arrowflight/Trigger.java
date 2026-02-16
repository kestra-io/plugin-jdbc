package io.kestra.plugin.jdbc.arrowflight;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;

import java.sql.DriverManager;
import java.sql.SQLException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for query results on Apache Arrow Flight SQL and trigger flow",
    description = "Periodically polls a database using Apache Arrow Flight SQL protocol by executing a SQL query at the specified interval (default 60 seconds). Triggers a downstream flow execution when the query returns one or more rows. Supports parameterized queries and afterSQL for marking processed rows. Use fetchType to control result handling."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a SQL query to return results, and then iterate through rows.",
            full = true,
            code = """
                id: jdbc_trigger
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.rows }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ json(taskrun.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.jdbc.arrowflight.Trigger
                    username: "{{ secret('DREMIO_USERNAME') }}"
                    password: "{{ secret('DREMIO_PASSWORD') }}"
                    url: jdbc:arrow-flight-sql://dremio-coordinator:32010/?schema=postgres.public
                    interval: "PT5M"
                    sql: "SELECT id, status FROM my_table WHERE status = 'NEW'"
                    afterSQL: "UPDATE my_table SET status = 'PROCESSED' WHERE status = 'NEW'"
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractJdbcTrigger implements ArrowFlightConnectionInterface {
    @Override
    protected AbstractJdbcQuery.Output runQuery(RunContext runContext) throws Exception {
        Query query = Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .url(this.getUrl())
            .username(this.getUsername())
            .password(this.getPassword())
            .timeZoneId(this.getTimeZoneId())
            .sql(this.getSql())
            .afterSQL(this.getAfterSQL())
            .store(this.isStore())
            .fetch(this.isFetch())
            .fetchOne(this.isFetchOne())
            .fetchType(Property.ofValue(this.renderFetchType(runContext)))
            .fetchSize(this.getFetchSize())
            .additionalVars(this.additionalVars)
            .parameters(this.getParameters())
            .build();
        return query.run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(ArrowFlightJdbcDriver.class::isInstance)) {
            DriverManager.registerDriver(new ArrowFlightJdbcDriver());
        }
    }
}
