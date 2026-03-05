package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.RetryUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import io.kestra.core.models.enums.MonacoLanguages;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;
import java.time.ZoneId;
import java.util.*;

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
        title = "Input file from internal storage",
        description = "URI of the source file (kestra://) containing rows to insert"
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "Parameterized INSERT statement to execute",
        description = """
            Prepared INSERT with ? placeholders for each bound column.
            Example: INSERT INTO <table_name> VALUES (?, ?, ?) for three columns; use column list if inserting a subset
            """
    )
    @PluginProperty(language = MonacoLanguages.SQL)
    private Property<String> sql;

    @Schema(
        title = "Batch size per executeBatch call",
        description = "Number of rows sent per JDBC batch before commit; default 1,000"
    )
    @Builder.Default
    @NotNull
    private Property<Integer> chunk = Property.ofValue(1000);

    @Schema(
        title = "Columns bound to placeholders",
        description = "Ordered column names matching ? placeholders; if omitted, placeholder count must match all columns in the input row"
    )
    private Property<List<String>> columns;

    @Schema(
        title = "Table used to auto-discover columns",
        description = """
            Retrieves column names from the given table when `columns` is empty.
            If `sql` is also omitted, an INSERT statement is generated automatically using the discovered columns
            """
    )
    private Property<String> table;
    
    @Schema(
        title = "Maximum number of retries for transient failures.",
        description = "Retries are attempted only for transient failures such as temporary I/O and recoverable SQL errors."
    )
    @Builder.Default
    private Property<Integer> maxRetries = Property.ofValue(3);

    @Schema(
        title = "Delay between retry attempts.",
        description = "Uses ISO-8601 duration format, for example `PT1S`."
    )
    @Builder.Default
    private Property<Duration> retryBackoff = Property.ofValue(Duration.ofSeconds(1));

    @Schema(
        title = "Input handling strategy.",
        description = """
            Controls how input is read during processing and retries.
            `AUTO` buffers small files locally (<= `localBufferMaxBytes`) and streams large files.
            `STREAM` always streams from internal storage.
            `LOCAL` always buffers input to a local temporary file before processing.
            """
    )
    @Builder.Default
    private Property<InputHandling> inputHandling = Property.ofValue(InputHandling.AUTO);

    @Schema(
        title = "Maximum number of bytes buffered locally.",
        description = """
            Used by `AUTO` and `LOCAL` input handling.
            In `AUTO`, files larger than this threshold are streamed.
            In `LOCAL`, files larger than this threshold fail fast.
            """
    )
    @Builder.Default
    private Property<Long> localBufferMaxBytes = Property.ofValue(100L * 1024L * 1024L);

    @Schema(
        title = "Resume from the last successfully committed chunk on retry."
    )
    @Builder.Default
    private Property<Boolean> resumeOnRetry = Property.ofValue(true);

    // will be used when killing
    @Getter(AccessLevel.NONE)
    private transient volatile Statement runningStatement;
    @Getter(AccessLevel.NONE)
    private transient volatile Connection runningConnection;

    protected abstract AbstractCellConverter getCellConverter(ZoneId zoneId);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        AbstractCellConverter converter = getCellConverter(zoneId(runContext));
        BatchConfig config = buildConfig(runContext);

        BatchExecutor executor = new BatchExecutor(config, runContext, converter, logger);

        try {
            logger.debug("Starting prepared statement: {}", config.sql());

            RetryUtils.of(buildRetryPolicy(runContext), logger)
                .runRetryIf(this::isRetryable, () -> {
                    executor.execute();
                    return null;
                });

            Output output = executor.output();

            runContext.metric(Counter.of("records", output.getRowCount()));
            runContext.metric(Counter.of("updated", output.getUpdatedCount()));

            logger.info(
                "Successfully executed {} bulk queries and updated {} rows",
                executor.getQueryCount(),
                output.getUpdatedCount()
            );

            return output;
        } finally {
            executor.cleanup();
            this.runningStatement = null;
            this.runningConnection = null;
        }
    }

    private BatchConfig buildConfig(RunContext runContext) throws Exception {
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        List<String> columnsToUse = runContext.render(this.columns).asList(String.class);
        String rTable = runContext.render(this.table).as(String.class).orElse(null);

        if (columnsToUse.isEmpty() && rTable != null) {
            columnsToUse = fetchColumnsFromTable(runContext, runContext.render(this.table).as(String.class).orElseThrow());
        }

        String rSql = runContext.render(this.sql).as(String.class).orElse(null);

        if (rSql == null && !columnsToUse.isEmpty()) {
            rSql = constructInsertStatement(runContext, rTable, columnsToUse);
        }

        return new BatchConfig(
            from,
            rSql,
            columnsToUse,
            runContext.render(this.chunk).as(Integer.class).orElse(1000),
            runContext.render(this.inputHandling).as(InputHandling.class).orElse(InputHandling.AUTO),
            runContext.render(this.localBufferMaxBytes).as(Long.class).orElse(100L * 1024L * 1024L),
            runContext.render(this.resumeOnRetry).as(Boolean.class).orElse(true)
        );
    }

    private Exponential buildRetryPolicy(RunContext runContext) throws IllegalVariableEvaluationException {
        return Exponential.builder()
            .interval(runContext.render(this.retryBackoff).as(Duration.class).orElse(Duration.ofSeconds(1)))
            .maxAttempts(runContext.render(this.maxRetries).as(Integer.class).orElse(3) + 1)
            .maxInterval(runContext.render(this.retryBackoff).as(Duration.class).orElse(Duration.ofSeconds(1)).multipliedBy(2))
            .build();
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

    private boolean isRetryable(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {

            if (cause instanceof SQLIntegrityConstraintViolationException || cause instanceof SQLSyntaxErrorException || cause instanceof SQLDataException || cause instanceof IllegalArgumentException) {
                return false;
            }

            if (cause instanceof IOException || cause instanceof SQLRecoverableException || cause instanceof SQLTransientException) {
                return true;
            }

            if (cause instanceof SQLException sql) {
                String state = sql.getSQLState();
                if (state != null && state.startsWith("08")) {
                    return true;
                }
            }
        }

        return false;
    }

    private PreparedInput prepareInput(RunContext runContext, URI from, InputHandling rInputHandling, long rLocalBufferMaxBytes, Logger logger) throws Exception {
        return switch (rInputHandling) {
            case STREAM -> PreparedInput.stream(() -> openRemoteReader(runContext, from));
            case LOCAL -> {
                Path localPath = bufferInputToLocal(runContext, from, rLocalBufferMaxBytes, true)
                    .orElseThrow(() -> new IllegalStateException("Input buffering failed in LOCAL mode."));
                logger.debug("Using LOCAL input handling for JDBC batch with local file {}", localPath);
                yield PreparedInput.local(localPath, () -> openLocalReader(localPath));
            }
            case AUTO -> {
                Optional<Path> localPath = bufferInputToLocal(runContext, from, rLocalBufferMaxBytes, false);
                if (localPath.isPresent()) {
                    logger.debug("AUTO input handling selected LOCAL mode for JDBC batch");
                    Path path = localPath.get();
                    yield PreparedInput.local(path, () -> openLocalReader(path));
                }

                logger.debug("AUTO input handling selected STREAM mode for JDBC batch");
                yield PreparedInput.stream(() -> openRemoteReader(runContext, from));
            }
        };
    }

    private Optional<Path> bufferInputToLocal(RunContext runContext, URI from, long rLocalBufferMaxBytes, boolean failIfTooLarge) throws Exception {
        Path localPath = runContext.workingDir().createTempFile(".ion");
        long copiedBytes = 0L;
        byte[] bytes = new byte[FileSerde.BUFFER_SIZE];

        try (
            var input = runContext.storage().getFile(from);
            var output = Files.newOutputStream(localPath)
        ) {
            int read;
            while ((read = input.read(bytes)) != -1) {
                copiedBytes += read;
                if (copiedBytes > rLocalBufferMaxBytes) {
                    if (failIfTooLarge) {
                        throw new IllegalArgumentException(
                            "Input file exceeds `localBufferMaxBytes` (" + rLocalBufferMaxBytes + " bytes) while using LOCAL input handling."
                        );
                    }

                    Files.deleteIfExists(localPath);
                    return Optional.empty();
                }
                output.write(bytes, 0, read);
            }
        } catch (Exception e) {
            Files.deleteIfExists(localPath);
            throw e;
        }

        return Optional.of(localPath);
    }

    private BufferedReader openRemoteReader(RunContext runContext, URI from) throws IOException {
        return new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE);
    }

    private BufferedReader openLocalReader(Path localPath) throws IOException {
        return new BufferedReader(new InputStreamReader(Files.newInputStream(localPath)), FileSerde.BUFFER_SIZE);
    }

    @SuppressWarnings("unchecked")
    private void addBatch(PreparedStatement ps, ParameterType parameterMetaData, Object row, List<String> columnsToUse, AbstractCellConverter cellConverter, Connection connection
    ) throws Exception {
        if (row instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) row;
            int index = 0;

            if (!columnsToUse.isEmpty()) {
                // If a column is missing from the current row, bind NULL explicitly
                // instead of skipping it. This ensures that all SQL placeholders '?'
                // receive a bound value, preventing ORA-17041.
                for (String col : columnsToUse) {
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
        } else if (row instanceof Collection) {
            ListIterator<Object> iter = ((List<Object>) row).listIterator();
            while (iter.hasNext()) {
                ps = cellConverter.addPreparedStatementValue(ps, parameterMetaData, iter.next(), iter.nextIndex(), connection);
            }
        }
        ps.addBatch();
    }

    public enum InputHandling {
        AUTO,
        STREAM,
        LOCAL
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
        @Schema(title = "Total rows read")
        private final Long rowCount;

        @Schema(title = "Rows inserted or updated")
        private final Integer updatedCount;
    }

    private record PreparedInput(InputHandling effectiveHandling, ReaderSupplier readerSupplier, Path localPath) {
        private static PreparedInput stream(ReaderSupplier readerSupplier) {
            return new PreparedInput(InputHandling.STREAM, readerSupplier, null);
        }

        private static PreparedInput local(Path localPath, ReaderSupplier readerSupplier) {
            return new PreparedInput(InputHandling.LOCAL, readerSupplier, localPath);
        }

        private boolean isLocal() {
            return this.localPath != null;
        }

        private void cleanup() throws IOException {
            if (this.localPath != null) {
                Files.deleteIfExists(this.localPath);
            }
        }
    }

    @FunctionalInterface
    private interface ReaderSupplier {
        BufferedReader open() throws Exception;
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

    final class BatchExecutor {

        private final BatchConfig config;
        private final RunContext runContext;
        private final AbstractCellConverter cellConverter;
        private final Logger logger;

        private long rowCount;
        private int updatedCount;
        private long queryCount;

        private PreparedInput preparedInput;

        BatchExecutor(
            BatchConfig config,
            RunContext runContext,
            AbstractCellConverter cellConverter,
            Logger logger
        ) {
            this.config = config;
            this.runContext = runContext;
            this.cellConverter = cellConverter;
            this.logger = logger;
        }

        void execute() throws Exception {
            long resumeOffset = config.resumeOnRetry() ? rowCount : 0L;

            if (preparedInput == null) {
                preparedInput = prepareInput(
                    runContext,
                    config.from(),
                    config.inputHandling(),
                    config.localBufferMaxBytes(),
                    logger
                );
            }

            executeAttempt(resumeOffset);
        }

        private void executeAttempt(long resumeOffset) throws Exception {
            try (
                Connection connection = connection(runContext);
                PreparedStatement ps = connection.prepareStatement(config.sql());
                BufferedReader reader = preparedInput.readerSupplier().open()
            ) {
                runningConnection = connection;
                runningStatement = ps;

                boolean supportsTx = connection.getMetaData().supportsTransactions();
                if (supportsTx) connection.setAutoCommit(false);

                ParameterType meta = ParameterType.of(ps.getParameterMetaData());
                List<Object> buffer = new ArrayList<>(config.chunk());
                long skip = resumeOffset;

                for (Object row : FileSerde.readAll(reader).toIterable()) {
                    if (skip-- > 0) continue;

                    buffer.add(row);
                    if (buffer.size() >= config.chunk()) {
                        flush(ps, meta, buffer, connection, supportsTx);
                    }
                }

                if (!buffer.isEmpty()) {
                    flush(ps, meta, buffer, connection, supportsTx);
                }
            }
        }

        private void flush(PreparedStatement ps, ParameterType meta, List<Object> rows, Connection connection, boolean supportsTx) throws Exception {
            for (Object row : rows) {
                addBatch(ps, meta, row, config.columns(), cellConverter, connection);
            }

            int[] updated = ps.executeBatch();
            if (supportsTx) connection.commit();
            ps.clearBatch();

            int size = rows.size();
            rows.clear();

            rowCount += size;
            updatedCount += Arrays.stream(updated).sum();
            queryCount++;
        }

        void cleanup() {
            if (preparedInput != null) {
                try {
                    preparedInput.cleanup();
                } catch (IOException e) {
                    logger.warn("Unable to cleanup local buffered input file", e);
                } finally {
                    preparedInput = null;
                }
            }
        }

        Output output() {
            return Output.builder()
                .rowCount(rowCount)
                .updatedCount(updatedCount)
                .build();
        }

        long getQueryCount() {
            return queryCount;
        }
    }

    record BatchConfig(URI from, String sql, List<String> columns, int chunk, InputHandling inputHandling, long localBufferMaxBytes, boolean resumeOnRetry) {}
}
