package io.kestra.plugin.jdbc.as400;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
    title = "Wait for query on a AS400 database."
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
                "    type: io.kestra.plugin.jdbc.as400.Trigger",
                "    interval: \"PT5M\"",
                "    url: jdbc:as400://127.0.0.1:50000/",
                "    username: as400_user",
                "    password: as400_passwd",
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
        DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());
    }
}
