package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.queues.QueueException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.kestra.plugin.jdbc.SqlSplitter.getQueries;

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

    protected void beforeExecute(RunContext runContext, Connection connection) throws Exception {
    }

    public MultiQueryOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        AbstractCellConverter cellConverter = getCellConverter(this.zoneId(runContext));

        final boolean isTransactional = runContext.render(this.transaction).as(Boolean.class).orElseThrow();
        long totalSize = 0L;
        List<Output> outputList = new LinkedList<>();
        List<Long> affectedRows = new ArrayList<>();
        var stats = new RunStats();
        var startedAt = System.nanoTime();

        // Create connection in not autocommit mode to enable rollback on error
        Savepoint savepoint = null;
        boolean supportsTx = false;

        try {
            this.runningConnection = this.connection(runContext);
            this.beforeExecute(runContext, this.runningConnection);
            supportsTx = supportsTransactions(this.runningConnection);
            final boolean useTransactions = supportsTx && isTransactional;

            if (useTransactions) {
                try {
                    this.runningConnection.setAutoCommit(false);
                } catch (SQLException e) {
                    runContext.logger().warn("Auto-commit disabling not supported: {}", e.getMessage());
                }
            }

            if (useTransactions) {
                savepoint = initializeSavepoint(this.runningConnection);
            }

            String rSql = runContext.render(this.sql).as(String.class, this.additionalVars).orElseThrow();
            FetchType fetchType = this.renderFetchType(runContext);
            boolean supportsMulti = supportsMultiStatements(this.runningConnection);

            boolean shouldBatchQueries = supportsMulti && useTransactions;
            var collector = columnStatsCollector(runContext, fetchType);

            String[] queries = shouldBatchQueries
                ? new String[]{rSql}
                : getQueries(rSql);

            for (String query : queries) {
                var statementStartedAt = System.nanoTime();
                var affectedBefore = affectedRows.size();
                // Create statement, execute
                try (PreparedStatement stmt = prepareStatement(runContext, this.runningConnection, query)) {
                    this.runningStatement = stmt;

                    if (fetchType == FetchType.STORE) {
                        stmt.setFetchSize(this.getFetchSize(runContext));
                    }

                    logger.debug("Starting query: {}", query);
                    boolean hasResultSet = stmt.execute();
                    if (!useTransactions && supportsTx && !this.runningConnection.getAutoCommit()) {
                        this.runningConnection.commit();
                    }
                    totalSize = extractResultsFromResultSet(
                        this.runningConnection,
                        stmt,
                        runContext,
                        cellConverter,
                        totalSize,
                        outputList,
                        affectedRows,
                        fetchType,
                        shouldBatchQueries,
                        hasResultSet,
                        collector,
                        stats
                    );
                    stats.warnings += JdbcExecutionSupport.countWarnings(stmt);
                    stats.recordStatement(query, affectedRows.subList(affectedBefore, affectedRows.size()), System.nanoTime() - statementStartedAt);

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
            var durationNanos = System.nanoTime() - startedAt;
            runContext.metric(Counter.of("fetch.size", totalSize, this.tags(runContext)));

            upsertAssets(runContext, queries, this.runningConnection);

            var statement = renderOutputStatement(runContext) ? JdbcExecutionSupport.maskedStatement(runContext, this.sql, rSql) : null;
            var columnStatsUri = collector == null || stats.statementLog.isEmpty() ? null : JdbcExecutionSupport.storeIon(runContext, collector.results());
            var rowsAffected = affectedRows.isEmpty() ? null : affectedRows.stream().mapToLong(Long::longValue).sum();
            var details = JdbcExecutionSupport.details(
                runContext,
                JdbcExecutionSupport.tables(this.runningConnection, Arrays.asList(queries)),
                statement,
                rowsAffected,
                stats.stored ? stats.bytes : null,
                durationNanos,
                stats.warnings,
                columnStatsUri,
                this.tags(runContext)
            );
            var statements = stats.statementLog.isEmpty() ? null : JdbcExecutionSupport.storeIon(runContext, stats.statementLog);

            var output = MultiQueryOutput.builder().outputs(outputList).affectedRows(affectedRows);
            MultiQueryOutput.applyDetails(output, details);
            return output
                .statementCount((long) stats.statementLog.size())
                .statements(statements)
                .build();
        } catch (Exception e) {
            if (supportsTx && isTransactional) {
                rollbackIfTransactional(this.runningConnection, savepoint);
            }
            throw new RuntimeException(e);
        } finally {
            closeConnection(runContext, this.runningConnection);
            this.runningConnection = null;
        }
    }

    private long extractResultsFromResultSet(final Connection connection,
                                             final PreparedStatement stmt,
                                             final RunContext runContext,
                                             final AbstractCellConverter cellConverter,
                                             long totalSize,
                                             final List<Output> outputList,
                                             final List<Long> affectedRows,
                                             final FetchType fetchType,
                                             final boolean multiStatements,
                                             final boolean hasResultSet,
                                             final ColumnStatsCollector collector,
                                             final RunStats stats
    ) throws SQLException, IOException {

        // ---------------------------------------------------------------------
        // Case 1: single statement execution (split mode)
        // - One execute() => at most one ResultSet
        // - Do NOT call getMoreResults() (some drivers like Pinot can hang)
        // - Do NOT call getUpdateCount() before consuming the ResultSet (Pinot can invalidate it)
        // ---------------------------------------------------------------------
        if (!multiStatements) {
            ResultSet rs = stmt.getResultSet();

            // When SQL is not a SELECT statement skip output creation, only record the update count
            if (rs == null) {
                var updateCount = JdbcExecutionSupport.updateCount(stmt);
                if (updateCount != null) {
                    affectedRows.add(updateCount);
                }
                return totalSize;
            }

            Output.OutputBuilder<?, ?> output = Output.builder();

            long size = 0L;
            switch (fetchType) {
                case FETCH_ONE -> {
                    var result = fetchResult(rs, cellConverter, connection);
                    size = result == null ? 0L : 1L;
                    output
                        .row(result)
                        .size(size);
                }
                case STORE -> {
                    File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                    try (var fileOutput = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
                        size = fetchToFile(stmt, rs, fileOutput, cellConverter, connection, collector);
                    }
                    stats.addStoredBytes(tempFile.length());
                    output
                        .uri(runContext.storage().putFile(tempFile))
                        .size(size);
                }
                case FETCH -> {
                    List<Map<String, Object>> maps = new ArrayList<>();
                    size = fetchResults(stmt, rs, maps, cellConverter, connection, collector);
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

            return totalSize;
        }

        // ---------------------------------------------------------------------
        // Case 2: multi statements execution (batched)
        // - One execute() => multiple results (ResultSet or updateCount)
        // - Use JDBC standard pattern: getResultSet/getUpdateCount + getMoreResults
        // ---------------------------------------------------------------------
        // Drive the loop off the boolean JDBC contract rather than re-probing getResultSet():
        // Snowflake keeps returning a non-null but already-closed ResultSet once the results are
        // drained, which made the old `rs != null` guard loop forever.
        boolean hasResults = hasResultSet;

        if (fetchType == FetchType.NONE) {
            runContext.logger().info("fetchType is set to NONE, no output will be returned");
        }

        while (true) {
            if (hasResults) {
                ResultSet rs = stmt.getResultSet();
                Output.OutputBuilder<?, ?> output = Output.builder();

                long size = 0L;
                switch (fetchType) {
                    case FETCH_ONE -> {
                        var result = fetchResult(rs, cellConverter, connection);
                        size = result == null ? 0L : 1L;
                        output
                            .row(result)
                            .size(size);
                    }
                    case STORE -> {
                        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                        try (var fileOutput = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
                            size = fetchToFile(stmt, rs, fileOutput, cellConverter, connection, collector);
                        }
                        stats.addStoredBytes(tempFile.length());
                        output
                            .uri(runContext.storage().putFile(tempFile))
                            .size(size);
                    }
                    case FETCH -> {
                        List<Map<String, Object>> maps = new ArrayList<>();
                        size = fetchResults(stmt, rs, maps, cellConverter, connection, collector);
                        output
                            .rows(maps)
                            .size(size);
                    }
                    case NONE -> { /* logged once before the loop */ }
                    default ->
                        throw new IllegalArgumentException("fetchType must be either FETCH, FETCH_ONE, STORE, or NONE");
                }

                totalSize += size;
                outputList.add(output.build());
            } else {
                var updateCount = JdbcExecutionSupport.updateCount(stmt);
                // No result set and no update count: end of results
                if (updateCount == null) {
                    break;
                }
                affectedRows.add(updateCount);
            }

            // Move to the next result
            hasResults = stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
        }

        return totalSize;
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
        var labels = columnLabels(rs);

        while (rs.next()) {
            Map<String, Object> map = super.mapResultSetToMap(rs, labels, cellConverter, connection);
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

    private void upsertAssets(RunContext runContext, String[] queries, Connection conn)
        throws SQLException, QueueException, IllegalVariableEvaluationException {

        for (String query : queries) {
            upsertAsset(runContext, conn, query);
        }
    }

    @Override
    public void kill() {
        super.kill(this.runningStatement);
        super.kill(this.runningConnection);
    }

    // Accumulates the figures that are only known while the statements run.
    private static final class RunStats {
        private final List<Map<String, Object>> statementLog = new ArrayList<>();
        private long bytes;
        private boolean stored;
        private int warnings;

        void addStoredBytes(long size) {
            bytes += size;
            stored = true;
        }

        void recordStatement(String query, List<Long> affected, long durationNanos) {
            var entry = new LinkedHashMap<String, Object>();
            entry.put("index", statementLog.size());
            entry.put("kind", SqlTableExtractor.statementKind(query));
            entry.put("rowsAffected", affected.isEmpty() ? null : affected.stream().mapToLong(Long::longValue).sum());
            entry.put("durationMs", Duration.ofNanos(durationNanos).toMillis());
            statementLog.add(entry);
        }
    }

    @SuperBuilder
    @Getter
    public static class MultiQueryOutput implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The list of per-query outputs")
        List<Output> outputs;

        @Schema(
            title = "The number of affected rows for each query that doesn't return a result set",
            description = "One entry per INSERT, UPDATE, DELETE or DDL statement, in execution order, as reported by the JDBC driver (DDL statements usually report 0). " +
                "Queries returning a result set are not included here; their results are in `outputs`."
        )
        List<Long> affectedRows;

        @Schema(
            title = "Total number of rows affected by all statements",
            description = "Sum of `affectedRows`. Not set when no statement reported an update count"
        )
        Long rowsAffected;

        @Schema(
            title = "Total size of the stored result files in bytes",
            description = "Only populated when fetchType is STORE"
        )
        Long bytes;

        @Schema(
            title = "Total execution duration in milliseconds",
            description = "Includes reading the result rows of every statement"
        )
        Long durationMs;

        @Schema(
            title = "Number of SQL warnings raised by the statements",
            description = "Count of the `SQLWarning` chains reported by the driver across all statements"
        )
        Integer warnings;

        @Schema(
            title = "Tables read or written by the statements",
            description = """
                Detected from the SQL text, so this is a best effort: CTEs, temporary tables and dialect specific syntax can lower the `confidence` or be missed.
                Entries found through the JDBC metadata have a `HIGH` confidence. Limited to 50 entries, see `tablesTruncated`"""
        )
        List<SqlTableExtractor.TableReference> tables;

        @Schema(
            title = "Whether the `tables` list was cut at 50 entries"
        )
        Boolean tablesTruncated;

        @Schema(
            title = "Executed SQL statements",
            description = "Only populated when `outputStatement` is true. Values of `secret()` calls are replaced by `***` and the text is truncated to 64 KiB"
        )
        String statement;

        @Schema(
            title = "URI of the column statistics file in internal storage",
            description = "Only populated when `columnStats` is true and fetchType is FETCH or STORE; columns of all result sets are merged by name"
        )
        URI columnStats;

        @Schema(
            title = "Number of executed statements",
            description = "When the driver runs the whole script in a single batch (transactional Snowflake, Redshift and Sybase scripts), the script counts as one statement"
        )
        Long statementCount;

        @Schema(
            title = "URI of the per-statement details file in internal storage",
            description = "ION file with one `{index, kind, rowsAffected, durationMs}` entry per executed statement"
        )
        URI statements;

        public void copyExecutionDetailsTo(MultiQueryOutputBuilder<?, ?> builder) {
            builder
                .rowsAffected(rowsAffected)
                .bytes(bytes)
                .durationMs(durationMs)
                .warnings(warnings)
                .tables(tables)
                .tablesTruncated(tablesTruncated)
                .statement(statement)
                .columnStats(columnStats)
                .statementCount(statementCount)
                .statements(statements);
        }

        static void applyDetails(MultiQueryOutputBuilder<?, ?> builder, JdbcExecutionSupport.Details details) {
            builder
                .rowsAffected(details.rowsAffected())
                .bytes(details.bytes())
                .durationMs(details.durationMs())
                .warnings(details.warnings())
                .tables(details.tables().tables())
                .tablesTruncated(details.tables().truncated() ? Boolean.TRUE : null)
                .statement(details.statement())
                .columnStats(details.columnStats());
        }
    }
}
