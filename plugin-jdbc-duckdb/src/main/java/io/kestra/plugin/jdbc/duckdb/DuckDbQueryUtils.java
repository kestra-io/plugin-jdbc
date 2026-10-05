package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.kestra.core.utils.Rethrow.throwConsumer;

abstract class DuckDbQueryUtils {
    record FileMetadata(long size, FileTime lastModifiedTime) {}

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
                stream.filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS))
                    .forEach(p -> {
                        try {
                            snapshot.put(p, new FileMetadata(Files.size(p), Files.getLastModifiedTime(p, LinkOption.NOFOLLOW_LINKS)));
                        } catch (IOException ignored) {
                        }
                    });
            } catch (IOException ignored) {
            }
        }
        return snapshot;
    }

    static void autoCaptureOutputFiles(
        RunContext runContext,
        Path workDir,
        Path databaseFile,
        Map<Path, FileMetadata> snapshot,
        Map<String, URI> uploaded,
        Set<Path> explicitOutputFilePaths
    ) throws Exception {
        if (workDir == null || !Files.exists(workDir)) {
            return;
        }

        var realWorkDir = workDir.toRealPath();
        var extDir = workDir.resolve(".duckdb_extensions").toAbsolutePath();

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
                .forEach(throwConsumer(path -> {
                    var relativeKey = workDir.relativize(path).toString().replace('\\', '/');
                    var fileUri = runContext.storage().putFile(path.toFile());
                    if (uploaded.putIfAbsent(relativeKey, fileUri) == null) {
                        runContext.logger().info("Captured output file: {}", relativeKey);
                    }
                }));
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
