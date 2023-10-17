package io.kestra.plugin.jdbc.snowflake;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@MicronautTest
@Disabled("no server for unit test")
public class UploadDownloadTest {
    @Value("${snowflake.host}")
    protected String host;

    @Value("${snowflake.username}")
    protected String username;

    @Value("${snowflake.password}")
    protected String password;

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Test
    void success() throws Exception {
        URL resource = UploadDownloadTest.class.getClassLoader().getResource("scripts/snowflake.sql");

        URI put = storageInterface.put(
            null,
            new URI("/file/storage/snowflake.sql"),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Upload upload = Upload.builder()
            .url("jdbc:snowflake://" + this.host + "/?loginTimeout=3")
            .username(this.username)
            .password(this.password)
            .warehouse("COMPUTE_WH")
            .database("UNITTEST")
            .from(put.toString())
            .schema("public")
            .stageName("UNITSTAGE")
            .prefix("ut_" + IdUtils.create())
            .fileName("test.sql")
            .build();

        Upload.Output uploadRun = upload.run(runContext);
        assertThat(uploadRun.getUri(), notNullValue());

        Download download = Download.builder()
            .url("jdbc:snowflake://" + this.host + "/?loginTimeout=3")
            .username(this.username)
            .password(this.password)
            .warehouse("COMPUTE_WH")
            .database("UNITTEST")
            .schema("public")
            .stageName("UNITSTAGE")
            .fileName(uploadRun.getUri().toString())
            .build();

        Download.Output downloadRun = download.run(runContext);
        assertThat(downloadRun.getUri(), notNullValue());

        assertThat(
            IOUtils.toString(this.storageInterface.get(null, downloadRun.getUri()), Charsets.UTF_8),
            is(IOUtils.toString(this.storageInterface.get(null, put), Charsets.UTF_8))
        );
    }
}
