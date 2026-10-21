package io.kestra.plugin.jdbc.explorer;

import io.kestra.core.plugins.endpoint.PluginEndpoint;
import io.kestra.core.plugins.endpoint.PluginEndpointContext;
import io.kestra.core.plugins.endpoint.PluginEndpointResponse;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Read-only SQL explorer over the database file exported by a task run (`databaseUri` output).
 * Everything is computed on demand from the file, which is copied once to a size capped temporary file.
 */
public abstract class AbstractReadOnlyExplorerEndpoint implements PluginEndpoint {
    public static final int MAX_LIMIT = 1000;
    public static final int DEFAULT_LIMIT = 100;
    public static final int MAX_CELL_LENGTH = 256;
    public static final int MAX_RESPONSE_BYTES = 5 * 1024 * 1024;
    public static final long MAX_FILE_BYTES = 256L * 1024 * 1024;
    private static final Duration FILE_TTL = Duration.ofMinutes(10);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final Pattern PATH_PATTERN = Pattern.compile("(?:[A-Za-z]:)?[\\\\/][^\\s'\"():;,]+");

    // The core calls handle() on the webserver IO thread with no timeout, so statements are cancelled from here.
    private static final ScheduledExecutorService WATCHDOG = Executors.newSingleThreadScheduledExecutor(runnable -> {
        var thread = new Thread(runnable, "jdbc-explorer-watchdog");
        thread.setDaemon(true);
        return thread;
    });

    public enum Operation {
        CATALOG,
        QUERY
    }

    private final ExplorerEngine engine;
    private final Operation operation;

    protected AbstractReadOnlyExplorerEndpoint(ExplorerEngine engine, Operation operation) {
        this.engine = engine;
        this.operation = operation;
    }

    @Override
    public String name() {
        return engine.name() + "-" + operation.name().toLowerCase();
    }

    protected Duration timeout() {
        return DEFAULT_TIMEOUT;
    }

    @Override
    public PluginEndpointResponse handle(PluginEndpointContext context) {
        Path file = null;
        try {
            file = databaseFile(context);
            try (var connection = engine.openReadOnly(file)) {
                var result = operation == Operation.CATALOG
                    ? catalog(connection)
                    : query(connection, context.body());
                return PluginEndpointResponse.ofBytes(capped(result), "application/json");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(sanitize(e.getMessage(), file));
        } catch (SQLException | IOException | RuntimeException e) {
            throw new IllegalArgumentException("The explorer could not run the request: " + sanitize(e.getMessage(), file));
        }
    }

    private Path databaseFile(PluginEndpointContext context) throws IOException {
        var outputs = context.outputs().get();
        var databaseUri = outputs == null ? null : outputs.get("databaseUri");
        if (databaseUri == null) {
            throw new IllegalArgumentException("This task run has no `databaseUri` output. Set `outputDbFile: true` on the task to explore its database.");
        }

        var directory = Path.of(System.getProperty("java.io.tmpdir"), "kestra-jdbc-explorer");
        Files.createDirectories(directory);
        purgeExpired(directory);

        var target = directory.resolve(fileKey(context.executionId(), context.taskRunId(), databaseUri.toString()));
        if (Files.exists(target)) {
            Files.setLastModifiedTime(target, java.nio.file.attribute.FileTime.from(java.time.Instant.now()));
            return target;
        }

        var partial = Files.createTempFile(directory, "download-", ".partial");
        try (InputStream input = context.storage().getFile(URI.create(databaseUri.toString()))) {
            copyCapped(input, partial);
            Files.move(partial, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(partial);
        }
        return target;
    }

    private static void copyCapped(InputStream input, Path target) throws IOException {
        var buffer = new byte[8192];
        var total = 0L;
        try (var output = Files.newOutputStream(target)) {
            int read;
            while ((read = input.read(buffer)) != -1) {
                total += read;
                if (total > MAX_FILE_BYTES) {
                    throw new IllegalArgumentException("The database file is larger than the explorer limit of " + MAX_FILE_BYTES / (1024 * 1024) + " MiB.");
                }
                output.write(buffer, 0, read);
            }
        }
    }

    private static void purgeExpired(Path directory) {
        var limit = System.currentTimeMillis() - FILE_TTL.toMillis();
        try (var files = Files.list(directory)) {
            for (var file : files.toList()) {
                if (Files.getLastModifiedTime(file).toMillis() < limit) {
                    Files.deleteIfExists(file);
                }
            }
        } catch (IOException ignored) {
            // Best effort: a file that cannot be removed now is retried on the next request.
        }
    }

    private static String fileKey(String executionId, String taskRunId, String uri) {
        try {
            var digest = MessageDigest.getInstance("SHA-256").digest((executionId + "|" + taskRunId + "|" + uri).getBytes());
            return HexFormat.of().formatHex(digest).substring(0, 32) + ".db";
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private Map<String, Object> catalog(Connection connection) throws SQLException {
        var tables = new LinkedHashMap<String, Map<String, Object>>();
        try (var statement = connection.createStatement()) {
            var watchdog = watch(statement);
            try (var rs = statement.executeQuery(engine.catalogSql())) {
                while (rs.next()) {
                    var key = rs.getString(1) + "." + rs.getString(2);
                    var table = tables.computeIfAbsent(key, k -> {
                        var created = new LinkedHashMap<String, Object>();
                        created.put("schema", cell(rs, 1));
                        created.put("name", cell(rs, 2));
                        created.put("type", cell(rs, 3));
                        created.put("columns", new ArrayList<Map<String, Object>>());
                        return created;
                    });
                    @SuppressWarnings("unchecked")
                    var columns = (List<Map<String, Object>>) table.get("columns");
                    columns.add(Map.of("name", String.valueOf(cell(rs, 4)), "type", String.valueOf(cell(rs, 5))));
                }
            } finally {
                watchdog.cancel(false);
            }
        }
        return Map.of("tables", new ArrayList<>(tables.values()));
    }

    private Map<String, Object> query(Connection connection, Map<String, Object> body) throws SQLException {
        var sql = body == null ? null : body.get("sql");
        if (!(sql instanceof String statement) || statement.isBlank()) {
            throw new IllegalArgumentException("The request body must contain a `sql` property.");
        }
        var limit = Math.min(MAX_LIMIT, Math.max(1, number(body.get("limit"), DEFAULT_LIMIT)));
        var offset = Math.max(0, number(body.get("offset"), 0));

        engine.validate(connection, statement);

        var startedAt = System.nanoTime();
        var columns = new ArrayList<Map<String, Object>>();
        var rows = new ArrayList<List<Object>>();
        var hasMore = false;

        try (var stmt = connection.createStatement()) {
            stmt.setMaxRows(offset + limit + 1);
            var watchdog = watch(stmt);
            try (ResultSet rs = stmt.executeQuery(statement)) {
                var meta = rs.getMetaData();
                for (var i = 1; i <= meta.getColumnCount(); i++) {
                    columns.add(Map.of("name", meta.getColumnLabel(i), "type", String.valueOf(meta.getColumnTypeName(i))));
                }
                var index = 0;
                while (rs.next()) {
                    if (index++ < offset) {
                        continue;
                    }
                    if (rows.size() == limit) {
                        hasMore = true;
                        break;
                    }
                    var row = new ArrayList<Object>(columns.size());
                    for (var i = 1; i <= columns.size(); i++) {
                        row.add(cell(rs, i));
                    }
                    rows.add(row);
                }
            } finally {
                watchdog.cancel(false);
            }
        }

        var result = new LinkedHashMap<String, Object>();
        result.put("columns", columns);
        result.put("rows", rows);
        result.put("hasMore", hasMore);
        result.put("durationMs", Duration.ofNanos(System.nanoTime() - startedAt).toMillis());
        return result;
    }

    private java.util.concurrent.ScheduledFuture<?> watch(Statement statement) {
        return WATCHDOG.schedule(() -> {
            try {
                statement.cancel();
            } catch (SQLException ignored) {
                // The statement already finished or the driver cannot cancel it.
            }
        }, timeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private static Object cell(ResultSet rs, int index) {
        try {
            var value = rs.getObject(index);
            if (value == null || value instanceof Number || value instanceof Boolean) {
                return value;
            }
            var text = value instanceof byte[] bytes ? "<" + bytes.length + " bytes>" : value.toString();
            return text.length() > MAX_CELL_LENGTH ? text.substring(0, MAX_CELL_LENGTH) + "..." : text;
        } catch (SQLException e) {
            return null;
        }
    }

    private static int number(Object value, int fallback) {
        return value instanceof Number number ? number.intValue() : fallback;
    }

    // Rows are dropped from the end until the JSON fits, and the page is flagged as incomplete.
    @SuppressWarnings("unchecked")
    private static byte[] capped(Map<String, Object> result) throws IOException {
        var mapper = JacksonMapper.ofJson();
        var bytes = mapper.writeValueAsBytes(result);
        var rows = result.get("rows") instanceof List<?> list ? (List<Object>) list : null;
        while (bytes.length > MAX_RESPONSE_BYTES && rows != null && !rows.isEmpty()) {
            rows.subList(Math.max(0, rows.size() / 2), rows.size()).clear();
            result.put("hasMore", true);
            bytes = mapper.writeValueAsBytes(result);
        }
        if (bytes.length > MAX_RESPONSE_BYTES) {
            throw new IllegalArgumentException("The response is larger than " + MAX_RESPONSE_BYTES / (1024 * 1024) + " MiB. Select fewer columns.");
        }
        return bytes;
    }

    private static String sanitize(String message, Path file) {
        var text = message == null ? "unknown error" : message;
        if (file != null) {
            text = text.replace(file.toString(), "<database>");
        }
        return PATH_PATTERN.matcher(text).replaceAll("<path>");
    }
}
