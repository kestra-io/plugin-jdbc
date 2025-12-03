package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Properties;


public interface SnowflakeInterface extends JdbcConnectionInterface {

    @PluginProperty(group = "connection")
    @Schema(
        title = "Specifies the virtual warehouse to use once connected.",
        description = "The specified warehouse should be an existing warehouse for which the specified default role has privileges.\n" +
            "If you need to use a different warehouse after connecting, execute the `USE WAREHOUSE` command to set a different warehouse for the session.")
    Property<String> getWarehouse();

    @PluginProperty(group = "connection")
    @Schema(
        title = "Specifies the default database to use once connected.",
        description = "The specified database should be an existing database for which the specified default role has privileges.\n" +
            "If you need to use a different database after connecting, execute the `USE DATABASE` command.")
    Property<String> getDatabase();

    @PluginProperty(group = "connection")
    @Schema(
        title = "Specifies the default schema to use for the specified database once connected.",
        description = "The specified schema should be an existing schema for which the specified default role has privileges.\n" +
            "If you need to use a different schema after connecting, execute the `USE SCHEMA` command.")
    Property<String> getSchema();

    @PluginProperty(group = "connection")
    @Schema(
        title = "Specifies the default access control role to use in the Snowflake session initiated by the driver.",
        description = "The specified role should be an existing role that has already been assigned to the specified user " +
            "for the driver. If the specified role has not already been assigned to the user, the role is not used when " +
            "the session is initiated by the driver.\n" +
            "If you need to use a different role after connecting, execute the `USE ROLE` command.")
    Property<String> getRole();

    @PluginProperty(group = "connection")
    @Schema(
        title = "Private key used for Snowflake key-pair authentication.",
        description = """
        Kestra supports multiple private key formats for Snowflake key-pair authentication.

        You can provide your key in any of the following formats:

        1. PKCS8 DER (base64-encoded, single-line)
        2. PEM PKCS8:
           -----BEGIN PRIVATE KEY-----
           ...
           -----END PRIVATE KEY-----

        3. PEM PKCS1 RSA:
           -----BEGIN RSA PRIVATE KEY-----
           ...
           -----END RSA PRIVATE KEY-----

        4. Multiline or single-line input (Kestra will normalize automatically)
        5. Encrypted PKCS8 (requires providing `privateKeyPassword`)

        ## Recommended format

        Snowflake recommends PKCS8.
        If your key is in PKCS1 format, Kestra will automatically convert it.

        ## Example: using a PEM PKCS8 key (recommended)
        secret('SNOWFLAKE_PRIVATE_KEY') should contain:

        -----BEGIN PRIVATE KEY-----
        MIIEvQIBADANBgkqhkiG9w0BAQEFAASC...
        ...
        -----END PRIVATE KEY-----

        ## Example: encrypted private key

        privateKey: "{{ secret('SNOWFLAKE_PRIVATE_KEY') }}"
        privateKeyPassword: "{{ secret('SNOWFLAKE_PRIVATE_KEY_PASSWORD') }}"

        ## Converting a PEM key to unencrypted PKCS8 DER (optional)

        openssl pkcs8 -topk8 -nocrypt -inform PEM -outform DER \\
          -in private_key.pem \\
          -out private_key.der

        base64 -w 0 private_key.der > private_key.base64

        You can then store the content of private_key.base64 as the Kestra secret.

        Kestra automatically detects the format and performs the necessary conversions.
        No manual header stripping or reformatting is required.
        """
    )
    Property<String> getPrivateKey();

    @PluginProperty(group = "connection")
    @Schema(
        title = "Specifies the private key password for key pair authentication and key rotation.")
    Property<String> getPrivateKeyPassword();

    @PluginProperty(dynamic = true)
    @Schema(
        title = "Query tag for Snowflake session tracking",
        description = "Optional string to tag queries executed within the session for monitoring and cost allocation"
    )
    Property<String> getQueryTag();

    /**
     * @deprecated use {@link #getPrivateKey()} instead
     */
    @Deprecated(since = "0.23.0", forRemoval = true)
    default void setPrivateKeyFile(Property<String> file){};

    /**
     * @deprecated use {@link #getPrivateKeyPassword()} instead
     */
    @Deprecated(since = "0.23.0", forRemoval = true)
    default void setPrivateKeyFilePassword(Property<String> pwd){};

    default void renderProperties(RunContext runContext, Properties properties) throws IllegalVariableEvaluationException {
        if (this.getWarehouse() != null) {
            properties.put("warehouse", runContext.render(this.getWarehouse()).as(String.class).orElseThrow());
        }

        if (this.getDatabase() != null) {
            properties.put("db", runContext.render(this.getDatabase()).as(String.class).orElseThrow());
        }

        if (this.getSchema() != null) {
            properties.put("schema", runContext.render(this.getSchema()).as(String.class).orElseThrow());
        }

        if (this.getRole() != null) {
            properties.put("role", runContext.render(this.getRole()).as(String.class).orElseThrow());
        }

        if (this.getPrivateKey() != null) {
            var unencryptedPrivateKey = RSAKeyPairUtils.deserializePrivateKey(
                runContext.render(this.getPrivateKey()).as(String.class).orElseThrow(),
                runContext.render(this.getPrivateKeyPassword()).as(String.class)
            );
            properties.put("privateKey", unencryptedPrivateKey);
        }

        if (this.getQueryTag() != null) {
            properties.put("query_tag", runContext.render(this.getQueryTag()).as(String.class).orElseThrow());
        }
    }

    @Override
    default String getScheme() {
        return "jdbc:snowflake";
    }
}
