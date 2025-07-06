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
        title = "Specifies the private key for key pair authentication and key rotation.",
        description = "It needs to be an un-encoded private key in plaintext like: 'MIIEvwIBADA...EwKx0TSWT9A=='")
    Property<String> getPrivateKey();

    @PluginProperty(group = "connection")
    @Schema(
        title = "Specifies the private key password for key pair authentication and key rotation.")
    Property<String> getPrivateKeyPassword();

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
    }

    @Override
    default String getScheme() {
        return "jdbc:snowflake";
    }
}
