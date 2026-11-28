package io.kestra.plugin.jdbc.openedge;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Poll Progress OpenEdge and trigger a flow",
    description = "Periodically executes a SQL query against OpenEdge and triggers a downstream flow execution when rows are returned. The proprietary DataDirect JDBC driver must be supplied separately."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a flow when new OpenEdge rows are found.",
            full = true,
            code = """
                id: openedge_trigger
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.Loop
                    values: "{{ trigger.rows }}"
                    tasks:
                      - id: log_row
                        type: io.kestra.plugin.core.log.Log
                        message: "{{ fromJson(item.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.jdbc.openedge.Trigger
                    interval: PT5M
                    url: jdbc:datadirect:openedge://erp-db.example.com:20000;databaseName=erp
                    username: "{{ secret('OPENEDGE_USERNAME') }}"
                    password: "{{ secret('OPENEDGE_PASSWORD') }}"
                    sql: SELECT * FROM PUB.orders WHERE status = 'NEW'
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractJdbcTrigger implements OpenEdgeConnectionInterface {
    @Getter(AccessLevel.NONE)
    protected transient Path workingDirectory;

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
            .afterSQL(this.getAfterSQL())
            .fetch(this.isFetch())
            .store(this.isStore())
            .fetchOne(this.isFetchOne())
            .fetchType(Property.ofValue(this.renderFetchType(runContext)))
            .skipExecutionDetails(true)
            .fetchSize(this.getFetchSize())
            .additionalVars(this.additionalVars)
            .parameters(this.getParameters())
            .build();

        return query.run(runContext);
    }
}
