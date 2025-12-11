package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.*;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcBatch extends Task implements RunnableTask<AbstractJdbcBatch.Output>, JdbcStatementInterface {

    @PluginProperty(group = "connection")
    private Property<String> url;

    @PluginProperty(group = "connection")
    private Property<String> username;

    @PluginProperty(group = "connection")
    private Property<String> password;

    @PluginProperty(group = "connection")
    private Property<String> timeZoneId;

    @NotNull
    @Schema(
        title = "Source file URI"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "Insert query to be executed.",
        description = """
            The query must have as many question marks as the number of columns in the table.
            Example: 'INSERT INTO <table_name> VALUES( ? , ? , ? )' for 3 columns.
            In case you do not want all columns, you need to specify it in the query in the columns property
            Example: 'INSERT INTO <table_name> (id, name) VALUES( ? , ? )' for inserting data into 2 columns: 'id' and 'name'.
            """
    )
    private Property<String> sql;

    @Schema(
        title = "The size of chunk for every bulk request."
    )
    @Builder.Default
    @NotNull
    private Property<Integer> chunk = Property.ofValue(1000);

    @Schema(
        title = "The columns to be inserted.",
        description = "If not provided, `?` count need to match the `from` number of columns."
    )
    private Property<List<String>> columns;

    @Schema(
        title = "The table from which column names will be retrieved.",
        description = """
            This property specifies the table name which will be used to retrieve the columns for the inserted values.
            You can use it instead of specifying manually the columns in the `columns` property. In this case, the `sql` property can also be omitted, an INSERT statement would be generated automatically.
            """
    )
    private Property<String> table;

    // will be used when killing
    @Getter(AccessLevel.NONE)
    private transient volatile Statement runningStatement;
    @Getter(AccessLevel.NONE)
    private transient volatile Connection runningConnection;

    protected abstract AbstractCellConverter getCellConverter(ZoneId zoneId);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        AtomicLong count = new AtomicLong();
        AtomicLong queryCount = new AtomicLong();

        AbstractCellConverter cellConverter = this.getCellConverter(this.zoneId(runContext));

        List<String> columnsToUse = runContext.render(this.columns).asList(String.class);
        if (columnsToUse.isEmpty() && this.table != null) {
            columnsToUse = fetchColumnsFromTable(runContext, runContext.render(this.table).as(String.class).orElseThrow());
        }

        String sql;
        if (!columnsToUse.isEmpty() && this.sql == null) {
            sql = constructInsertStatement(runContext, runContext.render(this.table).as(String.class).orElse(null), columnsToUse);
        } else {
            sql = runContext.render(this.sql).as(String.class).orElse(null);
        }

        logger.debug("Starting prepared statement: {}", sql);

        try (
            Connection connection = this.connection(runContext);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE);
            PreparedStatement ps = connection.prepareStatement(sql);
        ) {
            if (connection.getMetaData().supportsTransactions()) {
                connection.setAutoCommit(false);
            }

            int renderedChunk = runContext.render(this.chunk).as(Integer.class).orElseThrow();

            final ParameterType parameterMetaData = ParameterType.of(ps.getParameterMetaData());
            Integer updated = FileSerde.readAll(bufferedReader)
                .doOnNext(docWriteRequest -> count.incrementAndGet())
                .buffer(renderedChunk)
                .map(throwFunction(buffer -> {
                    for (Object row : buffer) {
                        addBatch(ps, parameterMetaData, row, cellConverter, connection, runContext);
                    }
                    int[] updatedRows = ps.executeBatch();
                    if (connection.getMetaData().supportsTransactions()) {
                        connection.commit();
                    }
                    queryCount.incrementAndGet();
                    return Arrays.stream(updatedRows).sum();
                }))
                .reduce(Integer::sum).block();

            runContext.metric(Counter.of("records", count.get()));
            runContext.metric(Counter.of("updated", updated == null ? 0 : updated));
            runContext.metric(Counter.of("query", queryCount.get()));

            logger.info("Successfully executed {} bulk queries and updated {} rows", queryCount.get(), updated);

            return Output
                .builder()
                .rowCount(count.get())
                .updatedCount(updated)
                .build();
        } finally {
            this.runningStatement = null;
            this.runningConnection = null;
        }
    }

    private String constructInsertStatement(RunContext runContext, String table, List<String> columns) throws IllegalVariableEvaluationException {
        return String.format(
            "INSERT INTO %s (%s) VALUES (%s)",
            runContext.render(table),
            String.join(", ", columns),
            String.join(", ", Collections.nCopies(columns.size(), "?"))
        );
    }

    private List<String> fetchColumnsFromTable(RunContext runContext, String table) throws Exception {
        List<String> columns = new ArrayList<>();

        try (Connection connection = this.connection(runContext)) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getColumns(null, null, table, null)) {
                while (resultSet.next()) {
                    columns.add(resultSet.getString("COLUMN_NAME"));
                }
            }
        }

        return columns;
    }

    @SuppressWarnings("unchecked")
    private void addBatch(
        PreparedStatement ps,
        ParameterType parameterMetaData,
        Object o,
        AbstractCellConverter cellConverter,
        Connection connection,
        RunContext runContext
    ) throws Exception {
        if (o instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) o;
            List<String> columnsValue = runContext.render(this.columns).asList(String.class);
            int index = 0;

            if (!columnsValue.isEmpty()) {
                // If a column is missing from the current row, bind NULL explicitly
                // instead of skipping it. This ensures that all SQL placeholders '?'
                // receive a bound value, preventing ORA-17041.
                for (String col : columnsValue) {
                    index++;
                    Object value = map.containsKey(col) ? map.get(col) : null;

                    // Explicit NULL binding safety: avoids "Unable to transform data with type 'null'"
                    if (value == null) {
                        Integer sqlType = parameterMetaData.getType(index);
                        ps.setNull(index, sqlType != null ? sqlType : java.sql.Types.NULL);
                    } else {
                        ps = cellConverter.addPreparedStatementValue(ps, parameterMetaData, value, index, connection);
                    }
                }
            } else {
                int expectedParams = parameterMetaData.types.size();
                for (String col : map.keySet()) {
                    index++;
                    ps = cellConverter.addPreparedStatementValue(ps, parameterMetaData, map.get(col), index, connection);
                }

                // Pad missing parameters with NULL until we reach the expected '?' count
                while (index < expectedParams) {
                    index++;
                    Integer sqlType = parameterMetaData.getType(index);
                    ps.setNull(index, sqlType != null ? sqlType : java.sql.Types.NULL);
                }
            }
        } else if (o instanceof Collection) {
            ListIterator<Object> iter = ((List<Object>) o).listIterator();
            while (iter.hasNext()) {
                ps = cellConverter.addPreparedStatementValue(ps, parameterMetaData, iter.next(), iter.nextIndex(), connection);
            }
        }
        ps.addBatch();
    }

    @Override
    public void kill() {
        try {
            if (this.runningStatement != null && !this.runningStatement.isClosed()) {
                this.runningStatement.cancel();
                this.runningStatement.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {
            if (this.runningConnection != null && !this.runningConnection.isClosed()) {
                this.runningConnection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The rows count.")
        private final Long rowCount;

        @Schema(title = "The updated rows count.")
        private final Integer updatedCount;
    }

    public static class ParameterType {
        private final Map<Integer, Class<?>> cls = new HashMap<>();
        private final Map<Integer, Integer> types = new HashMap<>();
        private final Map<Integer, String> typesName = new HashMap<>();

        public static ParameterType of(ParameterMetaData parameterMetaData) throws SQLException, ClassNotFoundException {
            ParameterType parameterType = new ParameterType();

            for (int i = 1; i <= parameterMetaData.getParameterCount(); i++) {
                parameterType.cls.put(i, Class.forName(parameterMetaData.getParameterClassName(i)));
                parameterType.types.put(i, parameterMetaData.getParameterType(i));
                parameterType.typesName.put(i, parameterMetaData.getParameterTypeName(i));
            }

            return parameterType;
        }

        public Class<?> getClass(int index) {
            return this.cls.get(index);
        }

        public Integer getType(int index) {
            return this.types.get(index);
        }

        public String getTypeName(int index) {
            return this.typesName.get(index);
        }
    }
}
