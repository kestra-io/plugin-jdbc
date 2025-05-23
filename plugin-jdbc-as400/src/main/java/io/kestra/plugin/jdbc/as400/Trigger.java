package io.kestra.plugin.jdbc.as400;

import com.ibm.as400.access.AS400JDBCDriver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Copied from the DB2 code as we cannot test AS400 we assume it works like DB2
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow if a periodically executed AS400 database query returns a non-empty result set."
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
                    type: io.kestra.plugin.jdbc.as400.Trigger
                    interval: "PT5M"
                    url: jdbc:as400://127.0.0.1:50000/
                    username: "{{ secret('AS400_USERNAME') }}"
                    password: "{{ secret('AS400_PASSWORD') }}"
                    sql: "SELECT * FROM my_table"
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractJdbcTrigger implements As400ConnectionInterface {

    @Override
    protected AbstractJdbcQuery.Output runQuery(RunContext runContext) throws Exception {

        var query = Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .url(this.getUrl())
            .username(this.getUsername())
            .password(this.getPassword())
            .timeZoneId(this.getTimeZoneId())
            .sql(this.getSql())
            .fetch(this.isFetch())
            .store(this.isStore())
            .fetchOne(this.isFetchOne())
            .fetchType(Property.of(this.renderFetchType(runContext)))
            .fetchSize(this.getFetchSize())
            .additionalVars(this.additionalVars)
            .parameters(this.getParameters())
            .build();

        return query.run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(AS400JDBCDriver.class::isInstance)) {
            DriverManager.registerDriver(new AS400JDBCDriver());
        }
    }
}
