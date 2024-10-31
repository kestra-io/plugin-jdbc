package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcQueries extends AbstractJdbcBaseQuery implements JdbcQueriesInterface {
    protected Property<Map<String, Object>> parameters;

    @Builder.Default
    protected Property<Boolean> transaction = Property.of(Boolean.TRUE);

    @Getter(AccessLevel.NONE)
    private Connection conn = null;

    @Getter(AccessLevel.NONE)
    private PreparedStatement stmt = null;

    @Getter(AccessLevel.NONE)
    private Savepoint savepoint = null;

    public AbstractJdbcQueries.MultiQueryOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        AbstractCellConverter cellConverter = getCellConverter(this.zoneId());

        final boolean isTransactional = this.transaction.as(runContext, Boolean.class);
        long totalSize = 0L;
        List<AbstractJdbcQuery.Output> outputList = new LinkedList<>();

        try  {
            //Create connection in not autocommit mode to enable rollback on error
            conn = this.connection(runContext);
            conn.setAutoCommit(false);
            savepoint = initializeSavepoint(conn);

            String sqlRendered = runContext.render(this.sql, this.additionalVars);
            String[] queries = sqlRendered.split(";[^']");

            for(String query : queries) {
                //Create statement, execute
                stmt = createPreparedStatementAndPopulateParameters(runContext, conn, query);
                stmt.setFetchSize(this.getFetchSize());
                logger.debug("Starting query: {}", query);
                stmt.execute();

                if(!isTransactional) {
                    conn.commit();
                }
                totalSize = extractResultsFromResultSet(runContext, cellConverter, totalSize, outputList);
            }
            conn.commit();

            runContext.metric(Counter.of("fetch.size",  totalSize, this.tags()));

            return MultiQueryOutput.builder().outputs(outputList).build();
        } catch (Exception e) {
            rollbackIfTransactional(isTransactional);
            throw new RuntimeException(e);
        } finally {
            closeConnectionAndStatement();
        }
    }

    private long extractResultsFromResultSet(RunContext runContext, AbstractCellConverter cellConverter, long totalSize, List<Output> outputList) throws SQLException, IOException {
        try(ResultSet rs = stmt.getResultSet()) {
            //When sql is not a select statement skip output creation
            if(rs != null) {
                Output.OutputBuilder<?, ?> output = Output.builder();
                //Populate result fro result set
                long size = 0L;
                switch (this.getFetchType()) {
                    case FETCH_ONE -> {
                        size = 1L;
                        output
                            .row(fetchResult(rs, cellConverter, conn))
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
                    default -> throw new IllegalArgumentException("fetchType must be either FETCH, FETCH_ONE, STORE, or NONE");
                }
                totalSize += size;
                outputList.add(output.build());
            }
        }
        return totalSize;
    }

    private void rollbackIfTransactional(boolean isTransactional) throws SQLException {
        if(isTransactional && conn != null) {
            if(savepoint != null) {
                conn.rollback(savepoint);
                return;
            }
            conn.rollback();
        }
    }

    private void closeConnectionAndStatement() throws SQLException {
        if(conn != null) { conn.close(); }
        if(stmt != null) { stmt.close(); }
    }

    private Savepoint initializeSavepoint(Connection conn) throws SQLException {
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

    @SuperBuilder
    @Getter
    public static class MultiQueryOutput implements io.kestra.core.models.tasks.Output {
        List<AbstractJdbcQuery.Output> outputs;
    }

    private PreparedStatement createPreparedStatementAndPopulateParameters(RunContext runContext, Connection conn, String sql) throws SQLException, IllegalVariableEvaluationException {
        //Inject named parameters (ex: ':param')
        Map<String, Object> namedParamsRendered = this.getParameters() == null ? null : this.getParameters().asMap(runContext, String.class, Object.class);

        if(namedParamsRendered == null || namedParamsRendered.isEmpty()) {
            return createPreparedStatement(conn, sql);
        }

        //Extract parameters in orders and replace them with '?'
        String preparedSql = sql;
        Pattern pattern = Pattern.compile(" :\\w+");
        Matcher matcher = pattern.matcher(preparedSql);

        List<String> params = new LinkedList<>();

        while (matcher.find()) {
            String param = matcher.group();
            params.add(param.substring(2));
            preparedSql = matcher.replaceFirst( " ?");
            matcher = pattern.matcher(preparedSql);
        }
        stmt = createPreparedStatement(conn, preparedSql);

        for(int i=0; i<params.size(); i++) {
            stmt.setObject(i+1, namedParamsRendered.get(params.get(i)));
        }

        return stmt;
    }

    protected PreparedStatement createPreparedStatement(Connection conn, String preparedSql) throws SQLException {
        return conn.prepareStatement(preparedSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }
}
