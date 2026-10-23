package io.kestra.plugin.jdbc.explorer;

import io.kestra.core.plugins.endpoint.PluginEndpoint;
import io.kestra.core.plugins.endpoint.PluginEndpointContext;
import io.kestra.core.plugins.endpoint.PluginEndpointResponse;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

// Read-only SQL explorer over the database file exported by a task run (`databaseUri` output).
public abstract class AbstractReadOnlyExplorerEndpoint implements PluginEndpoint {
    public static final int MAX_LIMIT = 1000;
    public static final int DEFAULT_LIMIT = 100;
    public static final int MAX_CELL_LENGTH = 256;
    public static final int MAX_RESPONSE_BYTES = 5 * 1024 * 1024;
    public static final long MAX_FILE_BYTES = 256L * 1024 * 1024;
    private static final Duration FILE_TTL = Duration.ofMinutes(10);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    public static final int MAX_OFFSET = 1_000_000;
    private static final long MAX_CACHE_BYTES = 1024L * 1024 * 1024;
    private static final Duration IN_USE_WINDOW = Duration.ofSeconds(60);
    // Absolute paths only, so `a/b` and `1/0` are kept.
    private static final Pattern PATH_PATTERN = Pattern.compile("(?<![^\\s'\"(=])(?:/[^\\s'\"():;,]+|[A-Za-z]:\\\\[^\\s'\"():;,]+)");

    // The core gives handle() no timeout, so statements are cancelled from here.
    private static final ScheduledExecutorService WATCHDOG = Executors.newSingleThreadScheduledExecutor(runnable -> {
        var thread = new Thread(runnable, "jdbc-explorer-watchdog");
        thread.setDaemon(true);
        return thread;
    });

    public enum Operation {
        CATALOG,
        QUERY
    }

    // Each request opens its own native engine, so parallel requests are bounded to protect the webserver.
    private static final Semaphore SLOTS = new Semaphore(3);

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
        var acquired = false;
        try {
            acquired = SLOTS.tryAcquire(5, TimeUnit.SECONDS);
            if (!acquired) {
                return error("The explorer is busy, retry in a moment.");
            }
            file = databaseFile(context);
            try (var connection = engine.openReadOnly(file)) {
                var result = operation == Operation.CATALOG
                    ? catalog(connection)
                    : query(connection, inputs(context));
                return PluginEndpointResponse.ofBytes(capped(result), "application/json");
            }
        } catch (IllegalArgumentException e) {
            return error(sanitize(e.getMessage(), file));
        } catch (SQLException | IOException | RuntimeException e) {
            return error("The explorer could not run the request: " + sanitize(e.getMessage(), file));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return error("The explorer request was interrupted.");
        } finally {
            if (acquired) {
                SLOTS.release();
            }
        }
    }

    // The core hides endpoint exception messages, so errors are returned as an `error` body.
    private static PluginEndpointResponse error(String message) {
        try {
            return PluginEndpointResponse.ofBytes(JacksonMapper.ofJson().writeValueAsBytes(Map.of("error", message)), "application/json");
        } catch (IOException e) {
            throw new IllegalStateException(e);
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

        var target = directory.resolve(fileKey(context.executionId(), context.taskRunId(), databaseUri.toString()));
        try {
            Files.setLastModifiedTime(target, FileTime.from(Instant.now()));
            return target;
        } catch (NoSuchFileException notCached) {
            // Not downloaded yet, or purged by a concurrent request: download it.
        }

        var partial = Files.createTempFile(directory, "download-", ".partial");
        try (InputStream input = context.storage().getFile(URI.create(databaseUri.toString()))) {
            copyCapped(input, partial);
            Files.move(partial, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(partial);
        }
        evict(directory, target);
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

    // Drops expired files, then the oldest until the cache fits, sparing files used in the last minute.
    private static void evict(Path directory, Path keep) {
        try (var stream = Files.list(directory)) {
            var files = new ArrayList<>(stream.filter(f -> !f.equals(keep) && !f.getFileName().toString().endsWith(".partial")).toList());
            files.sort(Comparator.comparingLong(AbstractReadOnlyExplorerEndpoint::modified));
            var total = Files.size(keep);
            for (var file : files) {
                total += Files.size(file);
            }
            var now = System.currentTimeMillis();
            for (var file : files) {
                var age = now - modified(file);
                var expired = age > FILE_TTL.toMillis();
                if ((expired || total > MAX_CACHE_BYTES) && age > IN_USE_WINDOW.toMillis()) {
                    total -= Files.size(file);
                    Files.deleteIfExists(file);
                }
            }
        } catch (IOException | UncheckedIOException ignored) {
            // Best effort: whatever is left is retried on the next download.
        }
    }

    private static long modified(Path file) {
        try {
            return Files.getLastModifiedTime(file).toMillis();
        } catch (IOException e) {
            return Long.MAX_VALUE;
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
        var offset = Math.min(MAX_OFFSET, Math.max(0, number(body.get("offset"), 0)));

        engine.validate(connection, statement);
        var executable = engine.boundCells(connection, statement);

        var startedAt = System.nanoTime();
        var columns = new ArrayList<Map<String, Object>>();
        var rows = new ArrayList<List<Object>>();
        var hasMore = false;

        try (var stmt = connection.createStatement()) {
            stmt.setMaxRows(offset + limit + 1);
            var watchdog = watch(stmt);
            try (ResultSet rs = stmt.executeQuery(executable)) {
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

    private ScheduledFuture<?> watch(Statement statement) {
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
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return value instanceof String text ? Integer.parseInt(text.trim()) : fallback;
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    // The host's fetchEndpoint sends the inputs as query parameters, a direct call sends a JSON body.
    private static Map<String, Object> inputs(PluginEndpointContext context) {
        var inputs = new java.util.HashMap<String, Object>();
        for (var name : List.of("sql", "limit", "offset")) {
            var value = context.param(name);
            if (value != null) {
                inputs.put(name, value);
            }
        }
        if (context.body() != null) {
            inputs.putAll(context.body());
        }
        return inputs;
    }

    // Rows are dropped from the end until the JSON fits.
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

    // Blanks string literals, quoted identifiers and comments so that keyword checks only see SQL code.
    public static String stripLiteralsAndComments(String sql) {
        var out = new StringBuilder(sql.length());
        var i = 0;
        while (i < sql.length()) {
            var c = sql.charAt(i);
            if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                var end = sql.indexOf('\n', i);
                i = end < 0 ? sql.length() : end;
                out.append(' ');
            } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                var end = sql.indexOf("*/", i + 2);
                i = end < 0 ? sql.length() : end + 2;
                out.append(' ');
            } else if (c == '\'' || c == '"' || c == '`') {
                var j = i + 1;
                while (j < sql.length()) {
                    if (sql.charAt(j) == c) {
                        if (j + 1 < sql.length() && sql.charAt(j + 1) == c) {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j++;
                }
                i = Math.min(sql.length(), j + 1);
                out.append(" \"\" ");
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    // Index of the last `;` that is not inside a literal, quoted identifier or comment, or -1.
    public static int lastTopLevelSemicolon(String sql) {
        var last = -1;
        var i = 0;
        while (i < sql.length()) {
            var c = sql.charAt(i);
            if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                var end = sql.indexOf('\n', i);
                i = end < 0 ? sql.length() : end;
            } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                var end = sql.indexOf("*/", i + 2);
                i = end < 0 ? sql.length() : end + 2;
            } else if (c == '\'' || c == '"' || c == '`') {
                var j = i + 1;
                while (j < sql.length()) {
                    if (sql.charAt(j) == c) {
                        if (j + 1 < sql.length() && sql.charAt(j + 1) == c) {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j++;
                }
                i = j + 1;
            } else {
                if (c == ';') {
                    last = i;
                }
                i++;
            }
        }
        return last;
    }
}
