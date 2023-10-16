package io.kestra.plugin.jdbc.dremio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AutoCommitInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.*;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Dremio database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a sql query to a Dremio database and fetch a row as outputs",
            code = {
                "url: jdbc:dremio:direct=sql.dremio.cloud:443;ssl=true;PROJECT_ID=sampleProjectId;",
                "username: $token",
                "password: samplePersonalAccessToken",
                "sql: select * FROM source.database.table",
                "fetchOne: true",
            }
        ),
        @Example(

        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface {
    protected final Boolean autoCommit = true;
    private boolean store = false;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DremioCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.dremio.jdbc.Driver());
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (store) {
            Output output = super.run(runContext);

            File tempFile = runContext.tempFile(".ion").toFile();
            saveIntoFile(output.getRows(), tempFile);

            return Output.builder()
                .size(output.getSize())
                .uri(runContext.putTempFile(tempFile))
                .build();
        }
        return super.run(runContext);
    }

    private void saveIntoFile(List<Map<String, Object>> rows, File tempFile) throws IOException {
        try (OutputStream output = new FileOutputStream(tempFile)) {
            rows.forEach(throwConsumer(row -> FileSerde.write(output, row)));
        }
    }


}
