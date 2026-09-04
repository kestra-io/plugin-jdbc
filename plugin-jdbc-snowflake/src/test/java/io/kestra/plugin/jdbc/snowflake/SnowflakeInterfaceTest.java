package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.hasKey;

@KestraTest
class SnowflakeInterfaceTest {
    @Inject
    private RunContextFactory runContextFactory;

    // --- quoteIdentifierIfNeeded (Fix B core logic, no infra needed) ---

    @Test
    void ordinaryIdentifierIsLeftUnquoted() {
        assertThat(SnowflakeInterface.quoteIdentifierIfNeeded("INGESTION"), is("INGESTION"));
        assertThat(SnowflakeInterface.quoteIdentifierIfNeeded("mydb"), is("mydb"));
        assertThat(SnowflakeInterface.quoteIdentifierIfNeeded("_private$1"), is("_private$1"));
    }

    @Test
    void specialCharacterIdentifierIsQuoted() {
        assertThat(
            SnowflakeInterface.quoteIdentifierIfNeeded("INGESTION:TOPIC_ENVIRONMENT"),
            is("\"INGESTION:TOPIC_ENVIRONMENT\"")
        );
        // a leading digit is not a legal unquoted identifier
        assertThat(SnowflakeInterface.quoteIdentifierIfNeeded("1db"), is("\"1db\""));
        // spaces require quoting
        assertThat(SnowflakeInterface.quoteIdentifierIfNeeded("my db"), is("\"my db\""));
    }

    @Test
    void alreadyQuotedIdentifierIsLeftUnchanged() {
        assertThat(
            SnowflakeInterface.quoteIdentifierIfNeeded("\"INGESTION:TOPIC_ENVIRONMENT\""),
            is("\"INGESTION:TOPIC_ENVIRONMENT\"")
        );
    }

    @Test
    void embeddedDoubleQuotesAreEscaped() {
        assertThat(SnowflakeInterface.quoteIdentifierIfNeeded("a\"b:c"), is("\"a\"\"b:c\""));
    }

    // --- renderProperties (Fix B end-to-end at the Properties level, as verified in the issue) ---

    @Test
    void renderPropertiesQuotesColonDatabaseButNotOrdinaryNames() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Query query = Query.builder()
            .url(Property.ofValue("jdbc:snowflake://acme.snowflakecomputing.com"))
            .warehouse(Property.ofValue("COMPUTE_WH"))
            .database(Property.ofValue("INGESTION:TOPIC_ENVIRONMENT"))
            .schema(Property.ofValue("PUBLIC"))
            .role(Property.ofValue("MY:ROLE"))
            .build();

        Properties properties = new Properties();
        query.renderProperties(runContext, properties);

        assertThat(properties.get("warehouse"), is("COMPUTE_WH"));
        assertThat(properties.get("db"), is("\"INGESTION:TOPIC_ENVIRONMENT\""));
        assertThat(properties.get("schema"), is("PUBLIC"));
        assertThat(properties.get("role"), is("\"MY:ROLE\""));
    }

    @Test
    void renderPropertiesOmitsUnsetIdentifiers() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Query query = Query.builder()
            .url(Property.ofValue("jdbc:snowflake://acme.snowflakecomputing.com"))
            .database(Property.ofValue("MYDB"))
            .build();

        Properties properties = new Properties();
        query.renderProperties(runContext, properties);

        assertThat(properties.get("db"), is("MYDB"));
        assertThat(properties, not(hasKey("schema")));
        assertThat(properties, not(hasKey("role")));
    }
}
