package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.duckdb.DuckDBDriver;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow if a periodically executed DuckDB query returns a non-empty result set."
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
                    type: io.kestra.plugin.jdbc.duckdb.Trigger
                    interval: "PT5M"
                    url: 'jdbc:duckdb:'
                    sql: "SELECT * FROM my_table"
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractJdbcTrigger implements DuckDbQueryInterface {

    @Getter(AccessLevel.NONE)
    private transient Path databaseFile;

    protected Object inputFiles;
    protected Property<List<String>> outputFiles;
    protected Property<String> databaseUri;

    @Builder.Default
    protected Property<Boolean> outputDbFile = Property.ofValue(false);

    @Override
    public Property<String> getUrl() {
        return Property.ofValue("jdbc:duckdb:" + databaseFile);
    }

    @Override
    protected AbstractJdbcQuery.Output runQuery(RunContext runContext) throws Exception {
        this.databaseFile = runContext.workingDir().createTempFile();
        Files.delete(this.databaseFile);

        additionalVars.put("workingDir", runContext.workingDir().path().toAbsolutePath().toString());

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
            .databaseUri(this.getDatabaseUri())
            .outputDbFile(this.getOutputDbFile())
            .inputFiles(this.getInputFiles())
            .build();
        return query.run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(DuckDBDriver.class::isInstance)) {
            DriverManager.registerDriver(new DuckDBDriver());
        }
    }
}
