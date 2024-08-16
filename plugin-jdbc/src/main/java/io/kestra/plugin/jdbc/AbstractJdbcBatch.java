package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.*;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcBatch extends Task implements JdbcStatementInterface {
    private String url;

    private String username;

    private String password;

    private String timeZoneId;

    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Insert query to be executed.",
        description = "The query must have as many question marks as the number of columns in the table." +
            "\nExample: 'insert into <table_name> values( ? , ? , ? )' for 3 columns." +
            "\nIn case you do not want all columns, you need to specify it in the query in the columns property" +
            "\nExample: 'insert into <table_name> (id, name) values( ? , ? )' for inserting data into 2 columns: 'id' and 'name'."
    )
    @PluginProperty(dynamic = true)
    private String sql;

    @Schema(
        title = "The size of chunk for every bulk request."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    @NotNull
    private Integer chunk = 1000;

    @Schema(
        title = "The columns to be inserted.",
        description = "If not provided, `?` count need to match the `from` number of columns."
    )
    @PluginProperty(dynamic = true)
    private List<String> columns;

    @Schema(
        title = "The table from which columns names will be retrieved.",
        description =
            "This property specifies table name, which will be used to retrieve columns to specify in what columns will be inserted values. \n" +
            "In That way columns names in insert statement will be match table schema."
    )
    @PluginProperty(dynamic = true)
    private String table;

    protected abstract AbstractCellConverter getCellConverter(ZoneId zoneId);

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from));

        AtomicLong count = new AtomicLong();

        AbstractCellConverter cellConverter = this.getCellConverter(this.zoneId());

        List<String> columnsToUse = this.columns;
        if (columnsToUse == null && this.table != null) {
            columnsToUse = fetchColumnsFromTable(runContext, this.table);
        }

        String sql;
        if (columnsToUse != null && this.sql == null) {
            sql = constructInsertStatement(runContext, this.table, columnsToUse);
        } else {
            sql = runContext.render(this.sql);
        }

        logger.debug("Starting prepared statement: {}", sql);

        try (
            Connection connection = this.connection(runContext);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE)
        ) {
            connection.setAutoCommit(false);

            Flux<Integer> flowable = FileSerde.readAll(bufferedReader)
                .doOnNext(docWriteRequest -> {
                    count.incrementAndGet();
                })
                .buffer(this.chunk, this.chunk)
                .map(throwFunction(o -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ParameterType parameterMetaData = ParameterType.of(ps.getParameterMetaData());

                    for (Object value : o) {
                        ps = this.addRows(ps, parameterMetaData, value, cellConverter, connection);

                        ps.addBatch();
                        ps.clearParameters();
                    }

                    int[] updatedRows = ps.executeBatch();
                    connection.commit();

                    return Arrays.stream(updatedRows).sum();
                }));

            Integer updated = flowable
                .reduce(Integer::sum)
                .block();

            runContext.metric(Counter.of("records", count.get()));
            runContext.metric(Counter.of("updated", updated == null ? 0 : updated));

            logger.info("Successfully executed {} bulk queries and updated {} rows", count.get(), updated);

            return Output
                .builder()
                .rowCount(count.get())
                .updatedCount(updated)
                .build();
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
    private PreparedStatement addRows(
        PreparedStatement ps,
        ParameterType parameterMetaData,
        Object o,
        AbstractCellConverter cellConverter,
        Connection connection
    ) throws Exception {
        if (o instanceof Map) {
            Map<String, Object> map = ((Map<String, Object>) o);
            ListIterator<String> iterKeys = new ArrayList<>(map.keySet()).listIterator();
            int index = 0;
            while (iterKeys.hasNext()) {
                String col = iterKeys.next();
                if (this.columns == null || this.columns.contains(col)) {
                    index++;
                    ps = cellConverter.addPreparedStatementValue(ps, parameterMetaData, map.get(col), index, connection);
                }
            }
        } else if (o instanceof Collection) {
            ListIterator<Object> iter = ((List<Object>) o).listIterator();

            while (iter.hasNext()) {
                ps = cellConverter.addPreparedStatementValue(ps, parameterMetaData, iter.next(), iter.nextIndex(), connection);
            }
        }

        return ps;
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
