package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Properties;

public interface SnowflakeInterface {
    @Schema(
        title = "Specifies the virtual warehouse to use once connected.",
        description = "The specified warehouse should be an existing warehouse for which the specified default role has privileges.\n" +
            "If you need to use a different warehouse after connecting, execute the `USE WAREHOUSE` command to set a different warehouse for the session."
    )
    @PluginProperty(dynamic = true)
    String getWarehouse();


    @Schema(
        title = "Specifies the default database to use once connected.",
        description = "The specified database should be an existing database for which the specified default role has privileges.\n" +
            "If you need to use a different database after connecting, execute the `USE DATABASE` command."
    )
    @PluginProperty(dynamic = true)
    String getDatabase();

    @Schema(
        title = "Specifies the default schema to use for the specified database once connected.",
        description = "The specified schema should be an existing schema for which the specified default role has privileges.\n" +
            "If you need to use a different schema after connecting, execute the `USE SCHEMA` command."
    )
    @PluginProperty(dynamic = true)
    String getSchema();

    @Schema(
        title = "Specifies the default access control role to use in the Snowflake session initiated by the driver.",
        description = "The specified role should be an existing role that has already been assigned to the specified user " +
            "for the driver. If the specified role has not already been assigned to the user, the role is not used when " +
            "the session is initiated by the driver.\n" +
            "If you need to use a different role after connecting, execute the `USE ROLE` command."
    )
    @PluginProperty(dynamic = true)
    String getRole();

    @Schema(
        title = "Specifies the private key for key pair authentication and key rotation.",
        description = "It needs to be an un-encoded private key in plaintext."
    )
    @PluginProperty(dynamic = true)
    String getPrivateKey();

    @Schema(
        title = "Specifies the private key file for key pair authentication and key rotation.",
        description = "It needs to be the path on the host where the private key file is located."
    )
    @PluginProperty(dynamic = true)
    String getPrivateKeyFile();

    @Schema(
        title = "Specifies the private key file password for key pair authentication and key rotation."
    )
    @PluginProperty(dynamic = true)
    String getPrivateKeyFilePassword();

    default void renderProperties(RunContext runContext, Properties properties) throws IllegalVariableEvaluationException {
        if (this.getWarehouse() != null) {
            properties.put("warehouse", runContext.render(this.getWarehouse()));
        }

        if (this.getDatabase() != null) {
            properties.put("db", runContext.render(this.getDatabase()));
        }

        if (this.getSchema() != null) {
            properties.put("schema", runContext.render(this.getSchema()));
        }

        if (this.getRole() != null) {
            properties.put("role", runContext.render(this.getRole()));
        }

        if (this.getPrivateKey() != null) {
            if (this.getPrivateKeyFile() != null || this.getPrivateKeyFilePassword() != null) {
                throw new IllegalArgumentException("The 'privateKeyFile' property cannot be used if the 'privateKey' property is used.")
            }
            properties.put("privateKey", runContext.render(this.getPrivateKey()));
        }

        if (this.getPrivateKeyFile() != null && this.getPrivateKeyFilePassword() != null) {
            properties.put("private_key_file", runContext.render(this.getPrivateKeyFile()));
            properties.put("private_key_file_pwd", runContext.render(this.getPrivateKeyFilePassword()));
        }
    }
}
