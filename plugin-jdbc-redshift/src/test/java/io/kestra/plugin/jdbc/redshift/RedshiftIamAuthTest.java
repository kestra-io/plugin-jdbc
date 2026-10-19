package io.kestra.plugin.jdbc.redshift;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class RedshiftIamAuthTest {
    private static final String PROVISIONED_URL = "jdbc:redshift://my-cluster.abc123xyz789.eu-central-1.redshift.amazonaws.com:5439/dev";
    private static final String SERVERLESS_URL = "jdbc:redshift://my-workgroup.123456789012.eu-central-1.redshift-serverless.amazonaws.com:5439/dev";

    @Inject
    private RunContextFactory runContextFactory;

    private RunContext runContext() {
        return runContextFactory.of(Map.of());
    }

    @Test
    void passwordAuthIsUnchanged() throws Exception {
        Query task = Query.builder()
            .url(Property.ofValue(PROVISIONED_URL))
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3str4"))
            .sql(Property.ofValue("select 1"))
            .build();

        Properties props = task.connectionProperties(runContext());

        assertThat(props.getProperty("jdbc.url"), is(PROVISIONED_URL));
        assertThat(props.getProperty("user"), is("kestra"));
        assertThat(props.getProperty("password"), is("k3str4"));
        assertThat(props.containsKey("AccessKeyID"), is(false));
    }

    @Test
    void accessKeysOnProvisionedCluster() throws Exception {
        Query task = Query.builder()
            .url(Property.ofValue(PROVISIONED_URL))
            .accessKeyId(Property.ofValue("AKIAEXAMPLE"))
            .secretKeyId(Property.ofValue("secret-example"))
            .sessionToken(Property.ofValue("token-example"))
            .region(Property.ofValue("eu-central-1"))
            .dbUser(Property.ofValue("analytics"))
            .autoCreate(Property.ofValue(true))
            .sql(Property.ofValue("select 1"))
            .build();

        Properties props = task.connectionProperties(runContext());

        assertThat(props.getProperty("jdbc.url"), is("jdbc:redshift:iam://my-cluster.abc123xyz789.eu-central-1.redshift.amazonaws.com:5439/dev"));
        assertThat(props.getProperty("AccessKeyID"), is("AKIAEXAMPLE"));
        assertThat(props.getProperty("SecretAccessKey"), is("secret-example"));
        assertThat(props.getProperty("SessionToken"), is("token-example"));
        assertThat(props.getProperty("Region"), is("eu-central-1"));
        assertThat(props.getProperty("DbUser"), is("analytics"));
        assertThat(props.getProperty("AutoCreate"), is("true"));
        assertThat(props.containsKey("password"), is(false));
        assertThat(props.containsKey("isServerless"), is(false));
    }

    @Test
    void dbUserDefaultsToUsername() throws Exception {
        Query task = Query.builder()
            .url(Property.ofValue(PROVISIONED_URL))
            .username(Property.ofValue("reporting"))
            .accessKeyId(Property.ofValue("AKIAEXAMPLE"))
            .secretKeyId(Property.ofValue("secret-example"))
            .sql(Property.ofValue("select 1"))
            .build();

        Properties props = task.connectionProperties(runContext());

        assertThat(props.getProperty("DbUser"), is("reporting"));
        assertThat(props.containsKey("user"), is(false));
    }

    @Test
    void serverlessWithDefaultCredentialsChain() throws Exception {
        Queries task = Queries.builder()
            .url(Property.ofValue(SERVERLESS_URL))
            .iamAuth(Property.ofValue(true))
            .workgroupName(Property.ofValue("my-workgroup"))
            .region(Property.ofValue("eu-central-1"))
            .dbUser(Property.ofValue("ignored-on-serverless"))
            .sql(Property.ofValue("select 1"))
            .build();

        Properties props = task.connectionProperties(runContext());

        assertThat(props.getProperty("jdbc.url"), startsWith("jdbc:redshift:iam://my-workgroup."));
        assertThat(props.getProperty("isServerless"), is("true"));
        assertThat(props.getProperty("serverlessWorkGroup"), is("my-workgroup"));
        assertThat(props.getProperty("Region"), is("eu-central-1"));
        assertThat(props.containsKey("AccessKeyID"), is(false));
        assertThat(props.containsKey("DbUser"), is(false));
    }

    @Test
    void credentialsNeverEndUpInTheUrl() throws Exception {
        Query task = Query.builder()
            .url(Property.ofValue(PROVISIONED_URL))
            .accessKeyId(Property.ofValue("AKIAEXAMPLE"))
            .secretKeyId(Property.ofValue("secret-example"))
            .sql(Property.ofValue("select 1"))
            .build();

        String url = task.connectionProperties(runContext()).getProperty("jdbc.url");

        assertThat(url, not(containsString("AKIAEXAMPLE")));
        assertThat(url, not(containsString("secret-example")));
    }

    @Test
    void accessKeyWithoutSecretIsRejected() {
        Query task = Query.builder()
            .url(Property.ofValue(PROVISIONED_URL))
            .accessKeyId(Property.ofValue("AKIAEXAMPLE"))
            .sql(Property.ofValue("select 1"))
            .build();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.connectionProperties(runContext()));
        assertThat(e.getMessage(), containsString("secretKeyId"));
    }

    @Test
    void iamUrlIsKeptAsIs() throws Exception {
        String iamUrl = "jdbc:redshift:iam://my-cluster:eu-central-1/dev";
        Query task = Query.builder()
            .url(Property.ofValue(iamUrl))
            .iamAuth(Property.ofValue(true))
            .sql(Property.ofValue("select 1"))
            .build();

        assertThat(task.connectionProperties(runContext()).getProperty("jdbc.url"), is(iamUrl));
    }

    @Test
    void triggerPassesIamPropertiesToItsQuery() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .url(Property.ofValue(SERVERLESS_URL))
            .accessKeyId(Property.ofValue("AKIAEXAMPLE"))
            .secretKeyId(Property.ofValue("secret-example"))
            .workgroupName(Property.ofValue("my-workgroup"))
            .sql(Property.ofValue("select 1"))
            .build();

        Properties props = trigger.connectionProperties(runContext());

        assertThat(props.getProperty("serverlessWorkGroup"), is("my-workgroup"));
        assertThat(props.getProperty("AccessKeyID"), is("AKIAEXAMPLE"));
    }
}
