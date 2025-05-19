package io.kestra.plugin.jdbc.dremio;

import com.dremio.jdbc.Driver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for query on a Dremio database."
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
                    type: io.kestra.plugin.core.flow.EachSequential
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ json(taskrun.value) }}"
                    value: "{{ trigger.rows }}"
                
                triggers:
                  - id: watch
                    type: io.kestra.plugin.jdbc.dremio.Trigger
                    interval: "PT5M"
                    url: jdbc:dremio:direct=sql.dremio.cloud:443;ssl=true;PROJECT_ID=sampleProjectId;
                    username: dremio_token
                    password: samplePersonalAccessToken
                    sql: "SELECT * FROM source.database.my_table"
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractJdbcTrigger {
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
            .store(this.isStore())
            .fetch(this.isFetch())
            .fetchOne(this.isFetchOne())
            .fetchType(this.getFetchType())
            .fetchSize(this.getFetchSize())
            .additionalVars(this.additionalVars)
            .build();
        return query.run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }
}
