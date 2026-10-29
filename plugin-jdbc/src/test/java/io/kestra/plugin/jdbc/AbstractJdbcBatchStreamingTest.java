package io.kestra.plugin.jdbc;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.assets.AssetsDeclaration;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.AssetFailureBehavior;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.AssetEmitter;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
        assertThat(output.getRowsAffected(), is((long) ROWS));
        assertThat(output.getDurationMs(), greaterThanOrEqualTo(0L));
        assertThat(output.getWarnings(), is(0));
        assertThat(output.getTables(), hasSize(1));
        assertThat(output.getTables().getFirst().getName(), is("streaming_batch"));
        assertThat(output.getTables().getFirst().getAccess(), is(SqlTableExtractor.Access.WRITE));
        assertThat(output.getTablesTruncated(), nullValue());
        assertThat(output.getStatement(), nullValue());
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
    void deprecatedConnectionPoolingPropertiesAreStillAccepted() throws Exception {
        var yaml = """
            id: test
            type: %s
            url: jdbc:h2:mem:test
            from: kestra:///file.ion
            sql: INSERT INTO t (id) VALUES (?)
            connectionPooling: true
            connectionPoolSize: 5
            """.formatted(StreamingBatch.class.getName());

        assertThat(io.kestra.core.serializers.JacksonMapper.ofYaml().readValue(yaml, StreamingBatch.class), is(notNullValue()));
    }

    @Test
    void shouldRollbackWhenBatchFails() throws Exception {
        String dbUrl = "jdbc:h2:mem:" + IdUtils.create() + ";DB_CLOSE_DELAY=-1";
        try (Connection setup = DriverManager.getConnection(dbUrl); Statement statement = setup.createStatement()) {
            statement.execute("CREATE TABLE rollback_batch (id INT, name VARCHAR(255))");
        }

        Path ionFile = Files.createTempFile("rollback_batch_", ".ion");
        try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(ionFile))) {
            for (int i = 0; i < 10; i++) {
                FileSerde.write(output, Map.of("id", i, "name", "row-" + i));
            }
        }
        URI uri;
        try (InputStream input = Files.newInputStream(ionFile)) {
            uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), input);
        }
        Files.delete(ionFile);

        AtomicInteger flushes = new AtomicInteger();
        AtomicInteger rollbacks = new AtomicInteger();
        StreamingBatch task = StreamingBatch.builder()
            .id(IdUtils.create())
            .type(StreamingBatch.class.getName())
            .url(Property.ofValue(dbUrl))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("INSERT INTO rollback_batch (id, name) VALUES (?, ?)"))
            .columns(Property.ofValue(List.of("id", "name")))
            .chunk(Property.ofValue(5))
            .onExecuteBatch(() -> {
                if (flushes.incrementAndGet() == 2) {
                    throw new IllegalStateException("boom");
                }
            })
            .onRollback(rollbacks::incrementAndGet)
            .build();

        assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of(Map.of())));

        assertThat(rollbacks.get(), is(1));
        try (Connection check = DriverManager.getConnection(dbUrl);
             Statement statement = check.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM rollback_batch")) {
            rs.next();
            assertThat(rs.getInt(1), is(5));
        }
    }

    @Test
    void assetFailureAfterCommitNeverReRunsTheInsert() throws Exception {
        String dbUrl = "jdbc:h2:mem:" + IdUtils.create() + ";DB_CLOSE_DELAY=-1";
        try (Connection setup = DriverManager.getConnection(dbUrl); Statement statement = setup.createStatement()) {
            statement.execute("CREATE TABLE asset_batch (id INT)");
        }

        Path ionFile = Files.createTempFile("asset_batch_", ".ion");
        try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(ionFile))) {
            for (int i = 0; i < 10; i++) {
                FileSerde.write(output, Map.of("id", i));
            }
        }
        URI uri;
        try (InputStream input = Files.newInputStream(ionFile)) {
            uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), input);
        }
        Files.delete(ionFile);

        AtomicInteger connections = new AtomicInteger();
        StreamingBatch task = StreamingBatch.builder()
            .id(IdUtils.create())
            .type(StreamingBatch.class.getName())
            .url(Property.ofValue(dbUrl))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("INSERT INTO asset_batch (id) VALUES (?)"))
            .columns(Property.ofValue(List.of("id")))
            .retryScope(Property.ofValue(AbstractJdbcBatch.RetryScope.ALL))
            .resumeOnRetry(Property.ofValue(false))
            .assets(new AssetsDeclaration(Property.ofValue(true), null, null, Property.ofValue(AssetFailureBehavior.FAIL)))
            .onConnection(() -> {
                // the first connection runs the insert, the second one is opened for the asset step
                if (connections.incrementAndGet() == 2) {
                    throw new SQLException("connection lost", "08006");
                }
            })
            .build();

        assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of(Map.of())));

        assertThat(connections.get(), is(2));
        try (Connection check = DriverManager.getConnection(dbUrl);
             Statement statement = check.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM asset_batch")) {
            rs.next();
            assertThat(rs.getInt(1), is(10));
        }
    }

    @Test
    void emitsTheWrittenTableWhenEnableAutoIsSet() throws Exception {
        var dbUrl = newTable("emit_batch");
        var emitter = new RecordingEmitter();

        var output = assetBatch(dbUrl, "emit_batch", new AssetsDeclaration(Property.ofValue(true), null, null, Property.ofValue(AssetFailureBehavior.FAIL)), null)
            .run(runContextWithEmitter(emitter));

        assertThat(output.getRowsAffected(), is(3L));
        assertThat(emitter.emitted(), hasSize(1));
        var asset = emitter.emitted().getFirst().outputs().getFirst();
        assertThat(asset.getId(), endsWith(".PUBLIC.EMIT_BATCH"));
        assertThat(asset.getMetadata().get("rowsAffected"), is(3L));
        assertThat(asset.getMetadata().get("columns"), is(notNullValue()));
    }

    @Test
    void emitsNothingAndOpensNoSecondConnectionWithoutEnableAuto() throws Exception {
        var dbUrl = newTable("no_auto_batch");
        var emitter = new RecordingEmitter();
        var connections = new AtomicInteger();

        assetBatch(dbUrl, "no_auto_batch", new AssetsDeclaration(Property.ofValue(false), null, null, null), connections::incrementAndGet)
            .run(runContextWithEmitter(emitter));

        assertThat(emitter.emitted(), is(empty()));
        assertThat(connections.get(), is(1));
    }

    @Test
    void unexpectedFailureWhileOpeningTheAssetConnectionIsAnAssetFailure() throws Exception {
        var dbUrl = newTable("runtime_batch");
        var connections = new AtomicInteger();
        var declaration = new AssetsDeclaration(Property.ofValue(true), null, null, Property.ofValue(AssetFailureBehavior.FAIL));

        var failing = assetBatch(dbUrl, "runtime_batch", declaration, () -> {
            if (connections.incrementAndGet() == 2) {
                throw new IllegalArgumentException("not a SQLException");
            }
        });
        var thrown = assertThrows(IllegalStateException.class, () -> failing.run(runContextFactory.of(Map.of())));
        assertThat(thrown.getMessage(), startsWith("Unable to emit assets"));

        var warnOnly = new AssetsDeclaration(Property.ofValue(true), null, null, null);
        var tolerant = assetBatch(dbUrl, "runtime_batch", warnOnly, () -> {
            if (connections.incrementAndGet() % 2 == 0) {
                throw new IllegalArgumentException("not a SQLException");
            }
        });
        assertThat(tolerant.run(runContextFactory.of(Map.of())).getRowsAffected(), is(3L));
    }

    private String newTable(String table) throws Exception {
        var dbUrl = "jdbc:h2:mem:" + IdUtils.create() + ";DB_CLOSE_DELAY=-1";
        try (var setup = DriverManager.getConnection(dbUrl); var statement = setup.createStatement()) {
            statement.execute("CREATE TABLE " + table + " (id INT)");
        }
        return dbUrl;
    }

    private StreamingBatch assetBatch(String dbUrl, String table, AssetsDeclaration assets, StreamingBatch.ThrowingRunnable onConnection) throws Exception {
        var ionFile = Files.createTempFile("asset_batch_", ".ion");
        try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(ionFile))) {
            for (var i = 0; i < 3; i++) {
                FileSerde.write(output, Map.of("id", i));
            }
        }
        URI uri;
        try (InputStream input = Files.newInputStream(ionFile)) {
            uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), input);
        }
        Files.delete(ionFile);

        return StreamingBatch.builder()
            .id(IdUtils.create())
            .type(StreamingBatch.class.getName())
            .url(Property.ofValue(dbUrl))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("INSERT INTO " + table + " (id) VALUES (?)"))
            .columns(Property.ofValue(List.of("id")))
            .assets(assets)
            .onConnection(onConnection)
            .build();
    }

    private RunContext runContextWithEmitter(AssetEmitter emitter) throws Exception {
        var runContext = spy(runContextFactory.of(Map.of()));
        doReturn(emitter).when(runContext).assets();
        return runContext;
    }

    private static final class RecordingEmitter implements AssetEmitter {
        private final List<AssetEmit> emitted = new ArrayList<>();

        @Override
        public void emit(AssetEmit emit) {
            emitted.add(emit);
        }

        @Override
        public List<AssetEmit> emitted() {
            return emitted;
        }
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class StreamingBatch extends AbstractJdbcBatch {
        private transient Runnable onExecuteBatch;
        private transient Runnable onRollback;
        private transient ThrowingRunnable onConnection;

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
            if (onConnection != null) {
                onConnection.run();
            }
            Connection connection = super.connection(runContext);
            return proxy(Connection.class, connection, (method, args) -> {
                if (method.getName().equals("rollback") && onRollback != null) {
                    onRollback.run();
                }
                Object result = method.invoke(connection, args);
                if (method.getName().equals("prepareStatement")) {
                    PreparedStatement ps = (PreparedStatement) result;
                    return proxy(PreparedStatement.class, ps, (psMethod, psArgs) -> {
                        if (psMethod.getName().equals("executeBatch") && onExecuteBatch != null) {
                            onExecuteBatch.run();
                        }
                        return psMethod.invoke(ps, psArgs);
                    });
                }
                return result;
            });
        }

        @FunctionalInterface
        public interface ThrowingRunnable {
            void run() throws SQLException;
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
