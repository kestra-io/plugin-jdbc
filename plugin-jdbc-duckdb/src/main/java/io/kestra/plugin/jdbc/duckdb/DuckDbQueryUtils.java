package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

abstract class DuckDbQueryUtils {
    record FileMetadata(long size, FileTime lastModifiedTime) {}

    private static final int MAX_PROCESS_DIR_ENTRIES = 1000;

    static Path processDirectory() {
        return Path.of(System.getProperty("user.dir"));
    }

    // When the working directory is the process directory, relative paths resolve to the very files the task created or may legitimately write.
    static boolean isSameDirectory(Path a, Path b) {
        if (a == null || b == null) {
            return false;
        }
        try {
            return a.toRealPath().equals(b.toRealPath());
        } catch (IOException | SecurityException e) {
            return a.toAbsolutePath().normalize().equals(b.toAbsolutePath().normalize());
        }
    }

    // DuckDB resolves relative paths against the process directory first, so a same-named file there shadows the input file.
    static void assertInputFilesNotShadowed(RunContext runContext, Path processDir, Path workingDirectory, Map<String, String> inputFiles, String rSql) {
        if (inputFiles == null || rSql == null || isSameDirectory(processDir, workingDirectory)) {
            return;
        }

        for (var key : inputFiles.keySet()) {
            Path normalizedPath;
            try {
                var keyPath = Path.of(key);
                if (keyPath.isAbsolute()) {
                    continue;
                }
                normalizedPath = keyPath.normalize();
            } catch (InvalidPathException e) {
                runContext.logger().debug("Skipping shadow check for input file '{}': {}", key, e.getMessage());
                continue;
            }

            var normalized = normalizedPath.toString().replace('\\', '/');
            if (normalized.isEmpty() || normalizedPath.startsWith("..")) {
                continue;
            }
            if (!rSql.contains("'" + normalized + "'") && !rSql.contains("'./" + normalized + "'")) {
                continue;
            }
            var stray = processDir.resolve(normalized).toAbsolutePath();
            if (Files.exists(stray)) {
                throw new IllegalStateException(String.format(
                    "The SQL references the input file '%s' with a relative path, but DuckDB resolves relative paths against the Kestra process directory " +
                        "and a file already exists there: '%s'. DuckDB would read that file instead of your input file. " +
                        "Delete the stray file or reference the input file as '{{ workingDir }}/%s'.",
                    normalized, stray, normalized
                ));
            }
        }
    }

    // Returns null when the directory is unreadable or too large: a partial snapshot would make the later comparison misleading.
    static Map<Path, FileMetadata> snapshotProcessDirectory(RunContext runContext, Path processDir, Path workingDirectory) {
        if (isSameDirectory(processDir, workingDirectory)) {
            return null;
        }
        try {
            return scanRegularFiles(processDir);
        } catch (IOException | SecurityException e) {
            runContext.logger().debug("Unable to snapshot the process directory '{}', skipping write detection: {}", processDir, e.getMessage());
            return null;
        } catch (TooManyFilesException e) {
            runContext.logger().debug("The process directory '{}' has more than {} files, skipping write detection", processDir, MAX_PROCESS_DIR_ENTRIES);
            return null;
        }
    }

    static void warnOnProcessDirectoryWrites(RunContext runContext, Path processDir, Map<Path, FileMetadata> snapshot) {
        if (snapshot == null) {
            return;
        }

        Map<Path, FileMetadata> current;
        try {
            current = scanRegularFiles(processDir);
        } catch (IOException | SecurityException e) {
            runContext.logger().debug("Unable to scan the process directory '{}' for written files: {}", processDir, e.getMessage());
            return;
        } catch (TooManyFilesException e) {
            runContext.logger().debug("The process directory '{}' has more than {} files, skipping write detection", processDir, MAX_PROCESS_DIR_ENTRIES);
            return;
        }

        var changed = new TreeSet<String>();
        for (var path : current.keySet()) {
            if (isNewOrModified(path, snapshot)) {
                changed.add(path.getFileName().toString());
            }
        }

        for (var name : changed) {
            runContext.logger().warn(
                "File '{}' was created or modified in the Kestra process directory '{}' and may have been written by this task through a relative path. " +
                    "It is not captured as an output file and is visible to other executions. " +
                    "Write to '{{ workingDir }}/{}' or '{{ outputFiles.<key> }}' instead.",
                name, processDir.toAbsolutePath(), name
            );
        }
    }

    private static Map<Path, FileMetadata> scanRegularFiles(Path processDir) throws IOException {
        var files = new HashMap<Path, FileMetadata>();
        try (var stream = Files.newDirectoryStream(processDir)) {
            for (var path : stream) {
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    continue;
                }
                if (files.size() >= MAX_PROCESS_DIR_ENTRIES) {
                    throw new TooManyFilesException();
                }
                files.put(path, new FileMetadata(Files.size(path), Files.getLastModifiedTime(path, LinkOption.NOFOLLOW_LINKS)));
            }
        }
        return files;
    }

    private static final class TooManyFilesException extends RuntimeException {
        TooManyFilesException() {
            super(null, null, false, false);
        }
    }

    static Map<Path, FileMetadata> takeSnapshot(
        RunContext runContext,
        Path workingDirectory,
        Property<Boolean> captureOutputFiles
    ) throws Exception {
        if (workingDirectory == null ||
            !workingDirectory.equals(runContext.workingDir().path()) ||
            !Boolean.TRUE.equals(runContext.render(captureOutputFiles).as(Boolean.class).orElse(true))) {
            return null;
        }

        var snapshot = new HashMap<Path, FileMetadata>();
        if (Files.exists(workingDirectory)) {
            try (var stream = Files.walk(workingDirectory)) {
                var iterator = stream.filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)).iterator();
                while (iterator.hasNext()) {
                    var p = iterator.next();
                    snapshot.put(p, new FileMetadata(Files.size(p), Files.getLastModifiedTime(p, LinkOption.NOFOLLOW_LINKS)));
                }
            }
        }
        return snapshot;
    }

    static Map<String, URI> uploadOutputFiles(
        RunContext runContext,
        Path workingDirectory,
        Path databaseFile,
        Map<String, String> explicitOutputFiles,
        Map<String, Object> additionalVars,
        Map<Path, FileMetadata> snapshot,
        Property<Integer> maxCapturedFiles,
        Property<Long> maxCapturedBytes
    ) throws Exception {
        var uploaded = new HashMap<String, URI>();
        var explicitOutputFilePaths = new HashSet<Path>();

        if (explicitOutputFiles != null) {
            explicitOutputFiles.forEach(throwBiConsumer((k, v) -> {
                var file = new File(runContext.render(v, additionalVars));
                uploaded.put(k, runContext.storage().putFile(file));
                explicitOutputFilePaths.add(file.toPath().toAbsolutePath());
            }));
        }

        if (snapshot != null) {
            autoCaptureOutputFiles(
                runContext,
                workingDirectory,
                databaseFile,
                snapshot,
                uploaded,
                explicitOutputFilePaths,
                maxCapturedFiles,
                maxCapturedBytes
            );
        }

        return uploaded;
    }

    private static void autoCaptureOutputFiles(
        RunContext runContext,
        Path workDir,
        Path databaseFile,
        Map<Path, FileMetadata> snapshot,
        Map<String, URI> uploaded,
        Set<Path> explicitOutputFilePaths,
        Property<Integer> maxCapturedFiles,
        Property<Long> maxCapturedBytes
    ) throws Exception {
        if (workDir == null || !Files.exists(workDir)) {
            return;
        }

        var realWorkDir = workDir.toRealPath();
        var extDir = workDir.resolve(".duckdb_extensions").toAbsolutePath();
        var candidates = new ArrayList<Path>();

        try (var stream = Files.walk(workDir)) {
            stream.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                .filter(path -> {
                    try {
                        return path.toRealPath().startsWith(realWorkDir);
                    } catch (IOException e) {
                        return false;
                    }
                })
                .filter(path -> {
                    if (databaseFile != null && path.toAbsolutePath().equals(databaseFile.toAbsolutePath())) {
                        return false;
                    }
                    var fileName = path.getFileName().toString();
                    return !fileName.endsWith(".db") && !fileName.endsWith(".wal") && !fileName.endsWith(".tmp");
                })
                .filter(path -> !path.toAbsolutePath().startsWith(extDir))
                .filter(path -> !explicitOutputFilePaths.contains(path.toAbsolutePath()))
                .filter(path -> isNewOrModified(path, snapshot))
                .forEach(candidates::add);
        }

        var maxFiles = runContext.render(maxCapturedFiles).as(Integer.class).orElse(100);
        if (candidates.size() > maxFiles) {
            throw new IllegalArgumentException(String.format(
                "Auto-capture exceeded maximum file count limit of %d (found %d files). " +
                "Configure `maxCapturedFiles` or set `captureOutputFiles: false` if intermediate files should not be uploaded.",
                maxFiles, candidates.size()
            ));
        }

        var maxBytes = runContext.render(maxCapturedBytes).as(Long.class).orElse(100L * 1024 * 1024);
        long totalBytes = 0;
        for (var path : candidates) {
            totalBytes += Files.size(path);
        }
        if (totalBytes > maxBytes) {
            throw new IllegalArgumentException(String.format(
                "Auto-capture exceeded maximum total size limit of %d bytes (found %d bytes). " +
                "Configure `maxCapturedBytes` or set `captureOutputFiles: false` if intermediate files should not be uploaded.",
                maxBytes, totalBytes
            ));
        }

        for (var path : candidates) {
            var relativeKey = workDir.relativize(path).toString().replace('\\', '/');
            var fileUri = runContext.storage().putFile(path.toFile());
            if (uploaded.putIfAbsent(relativeKey, fileUri) == null) {
                runContext.logger().info("Captured output file: {}", relativeKey);
            }
        }
    }

    private static boolean isNewOrModified(Path path, Map<Path, FileMetadata> snapshot) {
        var prev = snapshot.get(path);
        if (prev == null) {
            return true;
        }
        try {
            var currentSize = Files.size(path);
            var currentMtime = Files.getLastModifiedTime(path, LinkOption.NOFOLLOW_LINKS);
            return currentSize != prev.size() || !currentMtime.equals(prev.lastModifiedTime());
        } catch (IOException e) {
            return false;
        }
    }
}
