package io.kestra.plugin.jdbc.druid;

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
import org.apache.calcite.avatica.remote.Driver;

import java.sql.DriverManager;
import java.sql.SQLException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Wait for query on a Druid database."
)
@Plugin(
        examples = {
                @Example(
                        title = "Wait for a SQL query to return results, and then iterate through rows.",
                        full = true,
                        code = {
                                "id: jdbc_trigger",
                                "namespace: company.team",
                                "",
                                "tasks:",
                                "  - id: each",
                                "    type: io.kestra.plugin.core.flow.EachSequential",
                                "    tasks:",
                                "      - id: return",
                                "        type: io.kestra.plugin.core.debug.Return",
                                "        format: \"{{ json(taskrun.value) }}\"",
                                "    value: \"{{ trigger.rows }}\"",
                                "",
                                "triggers:",
                                "  - id: watch",
                                "    type: io.kestra.plugin.jdbc.druid.Trigger",
                                "    interval: \"PT5M\"",
                                "    url: jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/;transparent_reconnection=true",
                                "    sql: \"SELECT * FROM my_table\"",
                                "    fetch: true",
                        }
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
                .fetch(this.isFetch())
                .store(this.isStore())
                .fetchOne(this.isFetchOne())
                .fetchSize(this.getFetchSize())
                .additionalVars(this.additionalVars)
                .build();
        return query.run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new Driver());
    }
}

