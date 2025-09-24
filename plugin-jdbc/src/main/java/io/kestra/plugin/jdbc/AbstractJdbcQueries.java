package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Rethrow;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcQueries extends AbstractJdbcBaseQuery implements JdbcQueriesInterface {

    @Builder.Default
    protected Property<Boolean> transaction = Property.ofValue(Boolean.TRUE);

    public AbstractJdbcQueries.MultiQueryOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        AbstractCellConverter cellConverter = getCellConverter(this.zoneId(runContext));

        final boolean isTransactional = runContext.render(this.transaction).as(Boolean.class).orElseThrow();
        long totalSize = 0L;
        List<AbstractJdbcQuery.Output> outputList = new LinkedList<>();

        //Create connection in not autocommit mode to enable rollback on error
        Connection connection = null;
        Savepoint savepoint = null;
        boolean supportsTx = false;

        try {
            connection = this.connection(runContext);
            supportsTx = supportsTransactions(connection);
            final boolean useTransactions = supportsTx && isTransactional;

            if (supportsTx) {
                try {
                    connection.setAutoCommit(false);
                } catch (SQLException e) {
                    runContext.logger().warn("Auto-commit disabling not supported: {}", e.getMessage());
                }
            }

            if (isTransactional && supportsTx) {
                savepoint = initializeSavepoint(connection);
            }

            String sqlRendered = runContext.render(this.sql).as(String.class, this.additionalVars).orElseThrow();
            String[] queries = sqlRendered.split(";[^']");

            for (String query : queries) {
                //Create statement, execute
                try (PreparedStatement stmt = prepareStatement(runContext, connection, query)) {
                    stmt.setFetchSize(runContext.render(this.getFetchSize()).as(Integer.class).orElseThrow());
                    logger.debug("Starting query: {}", query);
                    stmt.execute();
                    if (!useTransactions && supportsTx) {
                        connection.commit();
                    }
                    totalSize = extractResultsFromResultSet(connection, stmt, runContext, cellConverter, totalSize, outputList);
                }
            }
            if (useTransactions) {
                connection.commit();
            }
            runContext.metric(Counter.of("fetch.size",  totalSize, this.tags(runContext)));

            return MultiQueryOutput.builder().outputs(outputList).build();
        } catch (Exception e) {
            if (supportsTx && isTransactional) {
                rollbackIfTransactional(connection, savepoint, true);
            }
            throw new RuntimeException(e);
        } finally {
            safelyCloseConnection(runContext, connection);
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
            //When sql is not a select statement skip output creation
            if (rs != null) {
                Output.OutputBuilder<?, ?> output = Output.builder();
                //Populate result fro result set
                long size = 0L;
                switch (this.renderFetchType(runContext)) {
                    case FETCH_ONE -> {
                        var result = fetchResult(rs, cellConverter, connection);
                        size = (result == null ? 0L : 1L);
                        output
                            .row(result)
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
            //Savepoint not supported by this driver
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


    @SuperBuilder
    @Getter
    public static class MultiQueryOutput implements io.kestra.core.models.tasks.Output {
        List<AbstractJdbcQuery.Output> outputs;
    }
}
