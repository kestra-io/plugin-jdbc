package io.kestra.plugin.jdbc;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcQuery extends AbstractJdbcBaseQuery {

    @Setter
    protected transient volatile Statement runningStatement;

    @Setter
    protected transient volatile Connection runningConnection;

    public AbstractJdbcBaseQuery.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        AbstractCellConverter cellConverter = getCellConverter(this.zoneId(runContext));

        String renderedSql = runContext.render(this.sql).as(String.class, this.additionalVars).orElseThrow();

        try (
            Connection conn = this.connection(runContext);
            Statement stmt = this.getParameters() == null ? this.createStatement(conn) : this.prepareStatement(runContext, conn, renderedSql)
        ) {
            this.runningConnection = conn;
            this.runningStatement = stmt;

            conn.setAutoCommit(true);
            stmt.setFetchSize(runContext.render(this.getFetchSize()).as(Integer.class).orElseThrow());

            logger.debug("Starting query: {}", renderedSql);

            boolean isResult = switch (stmt) {
                case PreparedStatement preparedStatement -> {
                    if (this.getParameters() == null) { // Null check for DuckDB which always use PreparedStatement
                        yield preparedStatement.execute(renderedSql);
                    }
                    yield preparedStatement.execute();
                }
                case Statement statement -> statement.execute(renderedSql);
            };

            Output.OutputBuilder<?, ?> output = AbstractJdbcBaseQuery.Output.builder();
            long size = 0L;

            if (isResult) {
                try (ResultSet rs = stmt.getResultSet()) {
                    //Populate result fro result set
                    switch (this.renderFetchType(runContext)) {
                        case FETCH_ONE -> {
                            var result = fetchResult(rs, cellConverter, conn);
                            size = result == null ? 0L : 1L;
                            output
                                .row(result)
                                .size(size);
                        }
                        case STORE -> {
                            File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                            try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
                                size = fetchToFile(stmt, rs, fileWriter, cellConverter, conn);
                            }
                            output
                                .uri(runContext.storage().putFile(tempFile))
                                .size(size);
                        }
                        case FETCH -> {
                            List<Map<String, Object>> maps = new ArrayList<>();
                            size = fetchResults(stmt, rs, maps, cellConverter, conn);
                            output
                                .rows(maps)
                                .size(size);
                        }
                    }
                }
            }
            runContext.metric(Counter.of("fetch.size", size, this.tags(runContext)));
            return output.build();
        } finally {
            this.runningStatement = null;
            this.runningConnection = null;
        }
    }
}
