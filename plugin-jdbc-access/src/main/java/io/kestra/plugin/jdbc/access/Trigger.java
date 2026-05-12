package io.kestra.plugin.jdbc.access;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import net.ucanaccess.jdbc.UcanaccessDriver;

import java.sql.DriverManager;
import java.sql.SQLException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for query results on a Microsoft Access database and trigger a flow",
    description = "Periodically polls a Microsoft Access database by executing a SQL query at the specified interval (default 60 seconds). Triggers a downstream flow execution when the query returns one or more rows. Supports parameterized queries and afterSQL for marking processed rows. Use fetchType to control result handling."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a SQL query to return results, and then iterate through rows.",
            full = true,
            code = """
                id: access_trigger
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
                    type: io.kestra.plugin.jdbc.access.Trigger
                    interval: "PT5M"
                    url: jdbc:ucanaccess:///myfile.accdb
                    sql: "SELECT * FROM my_table"
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractJdbcTrigger implements AccessQueryInterface {

    @PluginProperty(group = "source")
    protected Property<String> accessFile;

    @Builder.Default
    protected Property<Boolean> outputDbFile = Property.ofValue(false);

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
            .fetchSize(this.getFetchSize())
            .additionalVars(this.additionalVars)
            .parameters(this.getParameters())
            .outputDbFile(this.getOutputDbFile())
            .accessFile(this.getAccessFile())
            .build();
        return query.run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        if (DriverManager.drivers().noneMatch(UcanaccessDriver.class::isInstance)) {
            DriverManager.registerDriver(new UcanaccessDriver());
        }
    }
}
