package io.kestra.plugin.jdbc.explorer;

import io.kestra.core.plugins.endpoint.PluginEndpointContext;
import io.kestra.core.plugins.endpoint.ScopedStorage;
import io.kestra.core.plugins.endpoint.TaskRunLogsFetcher;
import io.kestra.core.plugins.endpoint.TaskRunOutputsFetcher;
import io.kestra.core.storages.FileAttributes;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

// Endpoint context for tests: the "internal storage" is a local file served for any URI.
public final class ExplorerTestContext {
    private ExplorerTestContext() {
    }

    public static final class NoOutputs {
        public PluginEndpointContext context(Map<String, Object> body) {
            return of(null, body);
        }
    }

    public static PluginEndpointContext of(Path databaseFile, Map<String, Object> body) {
        return new PluginEndpointContext() {
            @Override
            public Map<String, List<String>> parameters() {
                return Map.of();
            }

            @Override
            public Map<String, Object> body() {
                return body;
            }

            @Override
            public String executionId() {
                return "execution";
            }

            @Override
            public String taskRunId() {
                return "taskrun-" + UUID.randomUUID();
            }

            @Override
            public TaskRunLogsFetcher logs() {
                return level -> List.of();
            }

            @Override
            public TaskRunOutputsFetcher outputs() {
                return () -> databaseFile == null ? Map.of() : Map.of("databaseUri", "kestra:///db/" + databaseFile.getFileName());
            }

            @Override
            public ScopedStorage storage() {
                return new ScopedStorage() {
                    @Override
                    public InputStream getFile(URI uri) throws IOException {
                        return Files.newInputStream(databaseFile);
                    }

                    @Override
                    public List<FileAttributes> list(URI uri) {
                        return List.of();
                    }

                    @Override
                    public boolean exists(URI uri) {
                        return true;
                    }

                    @Override
                    public URI putFile(URI uri, InputStream input) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean deleteFile(URI uri) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
