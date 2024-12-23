package io.kestra.plugin.jdbc.snowflake;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
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

@KestraTest
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
            null,
            new URI("/file/storage/snowflake.sql"),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Upload upload = Upload.builder()
            .url(Property.of("jdbc:snowflake://" + this.host + "/?loginTimeout=3"))
            .username(Property.of(this.username))
            .password(Property.of(this.password))
            .warehouse(Property.of("COMPUTE_WH"))
            .database(Property.of("UNITTEST"))
            .from(Property.of(put.toString()))
            .schema(Property.of("public"))
            .stageName(Property.of("UNITSTAGE"))
            .prefix(Property.of("ut_" + IdUtils.create()))
            .fileName(Property.of("test.sql"))
            .build();

        Upload.Output uploadRun = upload.run(runContext);
        assertThat(uploadRun.getUri(), notNullValue());

        Download download = Download.builder()
            .url(Property.of("jdbc:snowflake://" + this.host + "/?loginTimeout=3"))
            .username(Property.of(this.username))
            .password(Property.of(this.password))
            .warehouse(Property.of("COMPUTE_WH"))
            .database(Property.of("UNITTEST"))
            .schema(Property.of("public"))
            .stageName(Property.of("UNITSTAGE"))
            .fileName(Property.of(uploadRun.getUri().toString()))
            .build();

        Download.Output downloadRun = download.run(runContext);
        assertThat(downloadRun.getUri(), notNullValue());

        assertThat(
            IOUtils.toString(this.storageInterface.get(null, null, downloadRun.getUri()), Charsets.UTF_8),
            is(IOUtils.toString(this.storageInterface.get(null, null, put), Charsets.UTF_8))
        );
    }
}
