package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class DuckDbTestUtils {
    public static URI getCsvSourceUri(StorageInterface storageInterface) throws URISyntaxException, IOException {
        return storageInterface.put(
            null,
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(new File(Objects.requireNonNull(DuckDbTest.class.getClassLoader()
                    .getResource("full.csv"))
                .toURI())
            )
        );
    }

    public static URI getDatabaseFileSourceUri(StorageInterface storageInterface) throws IOException, URISyntaxException {
        return storageInterface.put(
            null,
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(new File(Objects.requireNonNull(DuckDbTest.class.getClassLoader()
                    .getResource("db/file.db"))
                .toURI())
            )
        );
    }
}
