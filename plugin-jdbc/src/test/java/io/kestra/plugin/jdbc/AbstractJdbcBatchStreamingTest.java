package io.kestra.plugin.jdbc;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.Storage;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Verifies that {@link AbstractJdbcBatch} reads its input incrementally, so heap usage is bounded by {@code chunk}
 * and not by the size of the input file.
 */
@KestraTest
class AbstractJdbcBatchStreamingTest {
    private static final int ROWS = 100_000;
    private static final int CHUNK = 100;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void shouldFlushIncrementallyWhenInputExceedsChunkSize() throws Exception {
        String dbUrl = "jdbc:h2:mem:" + IdUtils.create() + ";DB_CLOSE_DELAY=-1";
        try (Connection setup = DriverManager.getConnection(dbUrl); Statement statement = setup.createStatement()) {
            statement.execute("CREATE TABLE streaming_batch (id INT, name VARCHAR(255))");
        }

        Path ionFile = Files.createTempFile("streaming_batch_", ".ion");
        try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(ionFile))) {
            for (int i = 0; i < ROWS; i++) {
                FileSerde.write(output, Map.of("id", i, "name", "row-" + i + "-some-padding-to-make-the-file-bigger"));
            }
        }
        long fileSize = Files.size(ionFile);
        URI uri;
        try (InputStream input = Files.newInputStream(ionFile)) {
            uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), input);
        }
        Files.delete(ionFile);

        // count the bytes pulled from internal storage
        AtomicLong bytesRead = new AtomicLong();
        RunContext runContext = spy(runContextFactory.of(Map.of()));
        Storage storage = spy(runContext.storage());
        doReturn(storage).when(runContext).storage();
        doAnswer(invocation -> new CountingInputStream((InputStream) invocation.callRealMethod(), bytesRead))
            .when(storage).getFile(any(URI.class));

        // record how many bytes had been read each time a chunk is sent to the database
        List<Long> bytesReadAtEachFlush = new ArrayList<>();
        StreamingBatch task = StreamingBatch.builder()
            .id(IdUtils.create())
            .type(StreamingBatch.class.getName())
            .url(Property.ofValue(dbUrl))
            .connectionPooling(Property.ofValue(false))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("INSERT INTO streaming_batch (id, name) VALUES (?, ?)"))
            .columns(Property.ofValue(List.of("id", "name")))
            .chunk(Property.ofValue(CHUNK))
            .inputHandling(Property.ofValue(AbstractJdbcBatch.InputHandling.STREAM))
            .onExecuteBatch(() -> bytesReadAtEachFlush.add(bytesRead.get()))
            .build();

        AbstractJdbcBatch.Output output = task.run(runContext);

        assertThat(output.getRowCount(), is((long) ROWS));
        assertThat(output.getUpdatedCount(), is(ROWS));
        assertThat(bytesReadAtEachFlush, hasSize(ROWS / CHUNK));
        assertThat(bytesRead.get(), is(fileSize));
        // the first chunk must be flushed after reading only a small part of the file, not the whole file
        assertThat(bytesReadAtEachFlush.getFirst(), lessThan(fileSize / 10));

        try (Connection check = DriverManager.getConnection(dbUrl);
             Statement statement = check.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM streaming_batch")) {
            rs.next();
            assertThat(rs.getInt(1), is(ROWS));
        }
    }

    @Test
    void shouldReadAllRowsWhenSeveralRowsShareOneLine() throws Exception {
        var dbUrl = createDatabase();
        var uri = store(ionRows(250, " ", false));
        var flushes = new ArrayList<Long>();

        var output = batch(dbUrl, uri, 100, AbstractJdbcBatch.InputHandling.STREAM, flushes).run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(250L));
        assertThat(output.getUpdatedCount(), is(250));
        assertThat(flushes, hasSize(3));
        assertThat(count(dbUrl), is(250));
    }

    @Test
    void shouldReadAllListRowsWrittenWithFileSerde() throws Exception {
        var dbUrl = createDatabase();
        var content = new ByteArrayOutputStream();
        for (var i = 0; i < 5; i++) {
            FileSerde.write(content, List.of(i, "row-" + i));
        }
        var uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new ByteArrayInputStream(content.toByteArray()));
        var flushes = new ArrayList<Long>();

        var output = batch(dbUrl, uri, 2, AbstractJdbcBatch.InputHandling.STREAM, flushes).run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(5L));
        assertThat(count(dbUrl), is(5));
    }

    @Test
    void shouldReadAllRowsWithLocalInputHandling() throws Exception {
        var dbUrl = createDatabase();
        var uri = store(ionRows(250, " ", true));
        var flushes = new ArrayList<Long>();

        var output = batch(dbUrl, uri, 100, AbstractJdbcBatch.InputHandling.LOCAL, flushes).run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(250L));
        assertThat(flushes, hasSize(3));
        assertThat(count(dbUrl), is(250));
    }

    @Test
    void shouldIgnoreBlankLinesAndTrailingNewlines() throws Exception {
        var dbUrl = createDatabase();
        var uri = store("\n" + ionRows(5, "\n\n", true) + "\n\n");
        var flushes = new ArrayList<Long>();

        var output = batch(dbUrl, uri, 100, AbstractJdbcBatch.InputHandling.STREAM, flushes).run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(5L));
        assertThat(flushes, hasSize(1));
        assertThat(count(dbUrl), is(5));
    }

    @Test
    void shouldSucceedWithoutExecutingBatchOnEmptyInput() throws Exception {
        var dbUrl = createDatabase();
        var uri = store("");
        var flushes = new ArrayList<Long>();

        var output = batch(dbUrl, uri, 100, AbstractJdbcBatch.InputHandling.STREAM, flushes).run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(0L));
        assertThat(output.getUpdatedCount(), is(0));
        assertThat(flushes, hasSize(0));
        assertThat(count(dbUrl), is(0));
    }

    @Test
    void shouldFailOnCorruptIonWithoutDuplicatingRowsOnRetry() throws Exception {
        var dbUrl = createDatabase();
        // 5 valid rows with chunk 2: rows 1-4 are committed, row 5 is buffered when the parser fails
        var uri = store(ionRows(5, "\n", true) + "{{{{ not ion");
        var flushes = new ArrayList<Long>();

        var task = batch(dbUrl, uri, 2, AbstractJdbcBatch.InputHandling.STREAM, flushes);

        assertThrows(Exception.class, () -> task.run(runContextFactory.of(Map.of())));
        // a wrong resume skip count would re-insert rows, which the primary key rejects or the count reveals
        assertThat(count(dbUrl), is(4));
    }

    private String createDatabase() throws SQLException {
        var dbUrl = "jdbc:h2:mem:" + IdUtils.create() + ";DB_CLOSE_DELAY=-1";
        try (var setup = DriverManager.getConnection(dbUrl); var statement = setup.createStatement()) {
            statement.execute("CREATE TABLE streaming_batch (id INT PRIMARY KEY, name VARCHAR(255))");
        }
        return dbUrl;
    }

    private static String ionRows(int rows, String separator, boolean trailingSeparator) {
        var content = new StringBuilder();
        for (var i = 0; i < rows; i++) {
            content.append("{id:").append(i).append(",name:\"row-").append(i).append("\"}");
            if (i < rows - 1 || trailingSeparator) content.append(separator);
        }
        return content.toString();
    }

    private URI store(String content) throws IOException {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
        );
    }

    private static StreamingBatch batch(String dbUrl, URI uri, int chunk, AbstractJdbcBatch.InputHandling handling, List<Long> flushes) {
        return StreamingBatch.builder()
            .id(IdUtils.create())
            .type(StreamingBatch.class.getName())
            .url(Property.ofValue(dbUrl))
            .connectionPooling(Property.ofValue(false))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("INSERT INTO streaming_batch (id, name) VALUES (?, ?)"))
            .columns(Property.ofValue(List.of("id", "name")))
            .chunk(Property.ofValue(chunk))
            .inputHandling(Property.ofValue(handling))
            .maxRetries(Property.ofValue(1))
            .retryBackoff(Property.ofValue(Duration.ofMillis(10)))
            .onExecuteBatch(() -> flushes.add(0L))
            .build();
    }

    private static int count(String dbUrl) throws SQLException {
        try (var check = DriverManager.getConnection(dbUrl);
             var statement = check.createStatement();
             var rs = statement.executeQuery("SELECT COUNT(*) FROM streaming_batch")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class StreamingBatch extends AbstractJdbcBatch {
        private transient Runnable onExecuteBatch;

        @Override
        protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
            return new AbstractCellConverter(zoneId) {
                @Override
                public Object convertCell(int columnIndex, ResultSet rs, Connection connection) {
                    return null;
                }
            };
        }

        @Override
        public void registerDriver() {
        }

        @Override
        public String getScheme() {
            return "jdbc:h2";
        }

        @Override
        public Connection connection(RunContext runContext) throws Exception {
            Connection connection = super.connection(runContext);
            return proxy(Connection.class, connection, (method, args) -> {
                Object result = method.invoke(connection, args);
                if (method.getName().equals("prepareStatement")) {
                    PreparedStatement ps = (PreparedStatement) result;
                    return proxy(PreparedStatement.class, ps, (psMethod, psArgs) -> {
                        if (psMethod.getName().equals("executeBatch")) {
                            onExecuteBatch.run();
                        }
                        return psMethod.invoke(ps, psArgs);
                    });
                }
                return result;
            });
        }

        @FunctionalInterface
        private interface Delegate {
            Object invoke(java.lang.reflect.Method method, Object[] args) throws Throwable;
        }

        private static <T> T proxy(Class<T> type, T target, Delegate delegate) {
            return type.cast(Proxy.newProxyInstance(
                AbstractJdbcBatchStreamingTest.class.getClassLoader(),
                new Class<?>[]{type},
                (proxy, method, args) -> {
                    try {
                        return delegate.invoke(method, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
            ));
        }
    }

    private static class CountingInputStream extends FilterInputStream {
        private final AtomicLong count;

        CountingInputStream(InputStream in, AtomicLong count) {
            super(in);
            this.count = count;
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b != -1) count.incrementAndGet();
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) count.addAndGet(n);
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long skipped = super.skip(n);
            count.addAndGet(skipped);
            return skipped;
        }
    }
}
