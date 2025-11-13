package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcQueries extends AbstractJdbcBaseQuery implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput>, JdbcQueriesInterface {

    @Builder.Default
    protected Property<Boolean> transaction = Property.ofValue(Boolean.TRUE);

    // will be used when killing
    @Getter(AccessLevel.NONE)
    private transient volatile Statement runningStatement;

    @Getter(AccessLevel.NONE)
    private transient volatile Connection runningConnection;

    public AbstractJdbcQueries.MultiQueryOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        AbstractCellConverter cellConverter = getCellConverter(this.zoneId(runContext));

        final boolean isTransactional = runContext.render(this.transaction).as(Boolean.class).orElseThrow();
        long totalSize = 0L;
        List<AbstractJdbcQuery.Output> outputList = new LinkedList<>();

        // Create connection in not autocommit mode to enable rollback on error
        Savepoint savepoint = null;
        boolean supportsTx = false;

        try {
            this.runningConnection = this.connection(runContext);
            supportsTx = supportsTransactions(this.runningConnection);
            final boolean useTransactions = supportsTx && isTransactional;

            if (supportsTx) {
                try {
                    this.runningConnection.setAutoCommit(false);
                } catch (SQLException e) {
                    runContext.logger().warn("Auto-commit disabling not supported: {}", e.getMessage());
                }
            }

            if (isTransactional && supportsTx) {
                savepoint = initializeSavepoint(this.runningConnection);
            }

            String rSql = runContext.render(this.sql).as(String.class, this.additionalVars).orElseThrow();
            String[] queries = getQueries(rSql);

            for (String query : queries) {
                // Create statement, execute
                try (PreparedStatement stmt = prepareStatement(runContext, this.runningConnection, query)) {
                    this.runningStatement = stmt;
                    stmt.setFetchSize(runContext.render(this.getFetchSize()).as(Integer.class).orElseThrow());
                    logger.debug("Starting query: {}", query);
                    stmt.execute();
                    if (!useTransactions && supportsTx) {
                        this.runningConnection.commit();
                    }
                    totalSize = extractResultsFromResultSet(this.runningConnection, stmt, runContext, cellConverter, totalSize, outputList);

                    // if the task has been killed, avoid processing the next query
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                } finally {
                    this.runningStatement = null;
                }
            }
            if (useTransactions) {
                this.runningConnection.commit();
            }
            runContext.metric(Counter.of("fetch.size", totalSize, this.tags(runContext)));

            return MultiQueryOutput.builder().outputs(outputList).build();
        } catch (Exception e) {
            if (supportsTx && isTransactional) {
                rollbackIfTransactional(this.runningConnection, savepoint, true);
            }
            throw new RuntimeException(e);
        } finally {
            safelyCloseConnection(runContext, this.runningConnection);
            this.runningConnection = null;
        }
    }

    private static void safelyCloseConnection(final RunContext runContext, final Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            runContext.logger().warn("Issue when closing the connection : {}", e.getMessage());
        }
    }

    private long extractResultsFromResultSet(final Connection connection,
                                             final PreparedStatement stmt,
                                             final RunContext runContext,
                                             final AbstractCellConverter cellConverter,
                                             long totalSize,
                                             final List<Output> outputList) throws SQLException, IOException, IllegalVariableEvaluationException {
        try (ResultSet rs = stmt.getResultSet()) {
            // When SQL is not a SELECT statement skip output creation
            if (rs != null) {
                Output.OutputBuilder<?, ?> output = Output.builder();
                // Populate result fro result set
                long size = 0L;
                switch (this.renderFetchType(runContext)) {
                    case FETCH_ONE -> {
                        size = 1L;
                        output
                            .row(fetchResult(rs, cellConverter, connection))
                            .size(size);
                    }
                    case STORE -> {
                        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                        try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
                            size = fetchToFile(stmt, rs, fileWriter, cellConverter, connection);
                        }
                        output
                            .uri(runContext.storage().putFile(tempFile))
                            .size(size);
                    }
                    case FETCH -> {
                        List<Map<String, Object>> maps = new ArrayList<>();
                        size = fetchResults(stmt, rs, maps, cellConverter, connection);
                        output
                            .rows(maps)
                            .size(size);
                    }
                    case NONE -> runContext.logger().info("fetchType is set to NONE, no output will be returned");
                    default ->
                        throw new IllegalArgumentException("fetchType must be either FETCH, FETCH_ONE, STORE, or NONE");
                }
                totalSize += size;
                outputList.add(output.build());
            }
        }
        return totalSize;
    }

    private static void rollbackIfTransactional(final Connection connection,
                                                final Savepoint savepoint,
                                                final boolean isTransactional) throws SQLException {
        if (isTransactional && connection != null) {
            if (savepoint != null) {
                connection.rollback(savepoint);
                return;
            }
            connection.rollback();
        }
    }

    private static Savepoint initializeSavepoint(final Connection conn) {
        try {
            return conn.setSavepoint();
        } catch (SQLException e) {
            // Savepoint not supported by this driver
            return null;
        }
    }

    @Override
    protected long fetch(Statement stmt, ResultSet rs, Consumer<Map<String, Object>> c, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        long count = 0L;

        while (rs.next()) {
            Map<String, Object> map = super.mapResultSetToMap(rs, cellConverter, connection);
            c.accept(map);
            count++;
        }

        return count;
    }

    private boolean supportsTransactions(Connection connection) {
        try {
            return connection.getMetaData().supportsTransactions();
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void kill() {
        super.kill(this.runningStatement);
        super.kill(this.runningConnection);
    }

    @SuperBuilder
    @Getter
    public static class MultiQueryOutput implements io.kestra.core.models.tasks.Output {
        List<AbstractJdbcQuery.Output> outputs;
    }
}
