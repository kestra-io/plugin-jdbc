package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class SqlServerSslPropertiesTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String DUMMY_URL = "jdbc:sqlserver://localhost:1433";

    @Test
    void defaultEncryptIsFalse() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var task = Query.builder()
            .url(Property.ofValue(DUMMY_URL))
            .sql(Property.ofValue("SELECT 1"))
            .build();

        var props = task.connectionProperties(runContext);

        assertThat(props.getProperty("encrypt"), is("false"));
    }

    @Test
    void explicitEncryptTrue() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var task = Query.builder()
            .url(Property.ofValue(DUMMY_URL))
            .sql(Property.ofValue("SELECT 1"))
            .encrypt(Property.ofValue(SqlServerConnectionInterface.EncryptMode.TRUE))
            .build();

        var props = task.connectionProperties(runContext);

        assertThat(props.getProperty("encrypt"), is("true"));
    }

    @Test
    void explicitEncryptStrict() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var task = Query.builder()
            .url(Property.ofValue(DUMMY_URL))
            .sql(Property.ofValue("SELECT 1"))
            .encrypt(Property.ofValue(SqlServerConnectionInterface.EncryptMode.STRICT))
            .build();

        var props = task.connectionProperties(runContext);

        assertThat(props.getProperty("encrypt"), is("strict"));
    }

    @Test
    void trustServerCertificateIsPassed() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var task = Query.builder()
            .url(Property.ofValue(DUMMY_URL))
            .sql(Property.ofValue("SELECT 1"))
            .trustServerCertificate(Property.ofValue(true))
            .build();

        var props = task.connectionProperties(runContext);

        assertThat(props.getProperty("trustServerCertificate"), is("true"));
    }

    @Test
    void allSslPropertiesArePassed() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var task = Query.builder()
            .url(Property.ofValue(DUMMY_URL))
            .sql(Property.ofValue("SELECT 1"))
            .encrypt(Property.ofValue(SqlServerConnectionInterface.EncryptMode.TRUE))
            .trustServerCertificate(Property.ofValue(true))
            .hostNameInCertificate(Property.ofValue("myhost.example.com"))
            .trustStore(Property.ofValue("/path/to/truststore.jks"))
            .trustStorePassword(Property.ofValue("s3cret"))
            .build();

        var props = task.connectionProperties(runContext);

        assertThat(props.getProperty("encrypt"), is("true"));
        assertThat(props.getProperty("trustServerCertificate"), is("true"));
        assertThat(props.getProperty("hostNameInCertificate"), is("myhost.example.com"));
        assertThat(props.getProperty("trustStore"), is("/path/to/truststore.jks"));
        assertThat(props.getProperty("trustStorePassword"), is("s3cret"));
    }

    @Test
    void noSslPropertiesWhenNullExceptDefaults() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var task = Query.builder()
            .url(Property.ofValue(DUMMY_URL))
            .sql(Property.ofValue("SELECT 1"))
            .encrypt(null)
            .trustServerCertificate(null)
            .build();

        var props = task.connectionProperties(runContext);

        // When encrypt is explicitly set to null (overriding the default), it should not be present
        assertThat(props.getProperty("encrypt"), is(nullValue()));
        assertThat(props.getProperty("trustServerCertificate"), is(nullValue()));
        assertThat(props.getProperty("hostNameInCertificate"), is(nullValue()));
        assertThat(props.getProperty("trustStore"), is(nullValue()));
        assertThat(props.getProperty("trustStorePassword"), is(nullValue()));
    }

    /**
     * BUG 1 regression: default @Builder.Default SSL props (encrypt=FALSE, trustServerCertificate=false)
     * must not overwrite parameters already present in the JDBC URL.
     * mssql-jdbc 13.x gives Properties precedence over URL params in mergeURLAndSuppliedProperties,
     * so injecting "false" into Properties silently overwrites "trustServerCertificate=true" in the URL.
     */
    @Test
    void urlTrustServerCertificateNotOverriddenByDefault() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        // URL has trustServerCertificate=true; task uses default (false via @Builder.Default)
        var task = Query.builder()
            .url(Property.ofValue("jdbc:sqlserver://localhost:1433;trustServerCertificate=true"))
            .sql(Property.ofValue("SELECT 1"))
            .build();

        var props = task.connectionProperties(runContext);

        // The task default "false" must NOT be injected when the URL already sets the param.
        // After the fix: the property is absent from Properties so the URL value stands.
        assertThat(
            "default trustServerCertificate=false must not overwrite URL trustServerCertificate=true",
            props.getProperty("trustServerCertificate"), is(nullValue())
        );
    }

    @Test
    void urlEncryptNotOverriddenByDefault() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        // URL sets encrypt=true; task uses default (FALSE via @Builder.Default)
        var task = Query.builder()
            .url(Property.ofValue("jdbc:sqlserver://localhost:1433;encrypt=true"))
            .sql(Property.ofValue("SELECT 1"))
            .build();

        var props = task.connectionProperties(runContext);

        // The default "false" must not overwrite the URL's "true"
        assertThat(
            "default encrypt=false must not overwrite URL encrypt=true",
            props.getProperty("encrypt"), is(nullValue())
        );
    }

    @Test
    void noUrlParamStillGetsDefault() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        // URL has no SSL params; defaults must still be applied
        var task = Query.builder()
            .url(Property.ofValue(DUMMY_URL))
            .sql(Property.ofValue("SELECT 1"))
            .build();

        var props = task.connectionProperties(runContext);

        assertThat("encrypt default must be applied when URL has no encrypt param",
            props.getProperty("encrypt"), is("false"));
        assertThat("trustServerCertificate default must be applied when URL has no such param",
            props.getProperty("trustServerCertificate"), is("false"));
    }

}
