package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcQuery extends AbstractJdbcBaseQuery implements RunnableTask<AbstractJdbcQuery.Output> {

    // will be used when killing
    @Getter(AccessLevel.NONE)
    private transient volatile Statement runningStatement;
    @Getter(AccessLevel.NONE)
    private transient volatile Connection runningConnection;

    @Override
    public AbstractJdbcBaseQuery.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        AbstractCellConverter cellConverter = getCellConverter(this.zoneId(runContext));

        Savepoint savepoint = null;
        boolean supportsTx = false;

        try (Connection conn = this.connection(runContext)) {
            this.runningConnection = conn;

            String rSql = runContext.render(this.sql).as(String.class, this.additionalVars).orElseThrow();
            long queriesAmount = countQueries(this.runningConnection, rSql);

            if (queriesAmount > 1) {
                throw new IllegalArgumentException(
                    "Query task support only a single SQL statement. Use the Queries task to run multiple statements."
                );
            }

            supportsTx = this.runningConnection.getMetaData().supportsTransactions();

            if (supportsTx) {
                conn.setAutoCommit(this.afterSQL == null);
                savepoint = (this.afterSQL != null) ? initializeSavepoint(conn) : null;
            }

            Output.OutputBuilder<?, ?> output = AbstractJdbcBaseQuery.Output.builder();
            long size = 0L;

            try (Statement stmt = this.getParameters() == null ? this.createStatement(this.runningConnection) : this.prepareStatement(runContext, this.runningConnection, rSql)) {
                this.runningStatement = stmt;

                stmt.setFetchSize(runContext.render(this.getFetchSize()).as(Integer.class).orElseThrow());

                logger.debug("Starting query: {}", rSql);

                boolean isResult = switch (stmt) {
                    case PreparedStatement preparedStatement -> {
                        if (this.getParameters() == null) { // Null check for DuckDB which always use PreparedStatement
                            yield preparedStatement.execute(rSql);
                        }
                        yield preparedStatement.execute();
                    }
                    case Statement statement -> statement.execute(rSql);
                };

                if (isResult) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        // Populate result from result set
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
            }

            executeAfterSQL(runContext, conn, logger, supportsTx);

            runContext.metric(Counter.of("fetch.size", size, this.tags(runContext)));
            return output.build();
        } catch (Exception e) {
            if (supportsTx && this.afterSQL != null) {
                rollbackIfTransactional(this.runningConnection, savepoint);
            }
            throw e;
        } finally {
            this.runningConnection = null;
        }
    }

    private long countQueries(Connection connection, String rSql) {
        if (supportsMultiStatements(connection)) {
            return 1;
        }

        return Arrays.stream(getQueries(rSql))
            .filter(s -> !s.isBlank())
            .filter(s -> !s.toLowerCase().startsWith("set file_search_path"))
            .count();
    }

    private void executeAfterSQL(RunContext runContext, Connection conn, Logger logger, boolean supportsTx) throws IllegalVariableEvaluationException, SQLException {
        // Execute afterSQL if present
        if (this.afterSQL != null) {
            String rAfterSQL = runContext.render(this.afterSQL).as(String.class, this.additionalVars).orElseThrow();

            long afterSQLStatements = Arrays.stream(rAfterSQL.split(";[^']"))
                .map(String::trim)
                .filter(s -> !s.isEmpty() && !s.toLowerCase().startsWith("set file_search_path"))
                .count();

            if (afterSQLStatements > 1) {
                throw new IllegalArgumentException(
                    "Query task afterSQL supports only a single SQL statement. Use the Queries task to run multiple statements."
                );
            }

            if (afterSQLStatements == 1) {
                try (Statement afterStmt = this.getParameters() == null ? this.createStatement(this.runningConnection) : this.prepareStatement(runContext, this.runningConnection, rAfterSQL)) {
                    logger.debug("Executing afterSQL: {}", rAfterSQL);

                    switch (afterStmt) {
                        case PreparedStatement preparedStatement -> {
                            // Null check for DuckDB which always use PreparedStatement
                            if (this.getParameters() == null) preparedStatement.execute(rAfterSQL);
                            preparedStatement.execute();
                        }
                        case Statement statement -> statement.execute(rAfterSQL);
                    }
                }

                // Commit if transaction was used
                if (supportsTx) {
                    conn.commit();
                }
            }
        }
    }

    @Override
    public void kill() {
        super.kill(this.runningStatement);
        super.kill(this.runningConnection);
    }

    private static Savepoint initializeSavepoint(final Connection conn) {
        try {
            return conn.setSavepoint();
        } catch (SQLException e) {
            //Savepoint not supported by this driver
            return null;
        }
    }

    private static void rollbackIfTransactional(final Connection connection,
                                                final Savepoint savepoint) throws SQLException {
        if (connection != null) {
            if (savepoint != null) {
                connection.rollback(savepoint);
                return;
            }
            connection.rollback();
        }
    }
}
