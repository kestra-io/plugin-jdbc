package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Properties;

public interface SnowflakeInterface extends JdbcConnectionInterface {
    @Schema(
        title = "Specifies the virtual warehouse to use once connected.",
        description = "The specified warehouse should be an existing warehouse for which the specified default role has privileges.\n" +
            "If you need to use a different warehouse after connecting, execute the `USE WAREHOUSE` command to set a different warehouse for the session.")
    Property<String> getWarehouse();


    @Schema(
        title = "Specifies the default database to use once connected.",
        description = "The specified database should be an existing database for which the specified default role has privileges.\n" +
            "If you need to use a different database after connecting, execute the `USE DATABASE` command.")
    Property<String> getDatabase();

    @Schema(
        title = "Specifies the default schema to use for the specified database once connected.",
        description = "The specified schema should be an existing schema for which the specified default role has privileges.\n" +
            "If you need to use a different schema after connecting, execute the `USE SCHEMA` command.")
    Property<String> getSchema();

    @Schema(
        title = "Specifies the default access control role to use in the Snowflake session initiated by the driver.",
        description = "The specified role should be an existing role that has already been assigned to the specified user " +
            "for the driver. If the specified role has not already been assigned to the user, the role is not used when " +
            "the session is initiated by the driver.\n" +
            "If you need to use a different role after connecting, execute the `USE ROLE` command.")
    Property<String> getRole();

    @Schema(
        title = "Specifies the private key for key pair authentication and key rotation.",
        description = "It needs to be an un-encoded private key in plaintext like: 'MIIEvwIBADA...EwKx0TSWT9A=='")
    Property<String> getPrivateKey();

    @Schema(
        title = "Specifies the private key file for key pair authentication and key rotation.",
        description = "It needs to be the path on the host where the private key file is located.")
    Property<String> getPrivateKeyFile();

    @Schema(
        title = "Specifies the private key file password for key pair authentication and key rotation.")
    Property<String> getPrivateKeyFilePassword();

    default void renderProperties(RunContext runContext, Properties properties) throws IllegalVariableEvaluationException, NoSuchAlgorithmException, InvalidKeySpecException {
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
            if (this.getPrivateKeyFile() != null || this.getPrivateKeyFilePassword() != null) {
                throw new IllegalArgumentException("The 'privateKeyFile' property cannot be used if the 'privateKey' property is used.");
            }
            var privateKeyBase64 = runContext.render(this.getPrivateKey()).as(String.class).orElseThrow();
            var privateKeyBytes = Base64.getDecoder().decode(privateKeyBase64.getBytes());
            var spec = new PKCS8EncodedKeySpec(privateKeyBytes);
            var keyFactory = KeyFactory.getInstance("RSA");
            properties.put("privateKey", keyFactory.generatePrivate(spec));
        }

        if (this.getPrivateKeyFile() != null && this.getPrivateKeyFilePassword() != null) {
            properties.put("private_key_file", runContext.render(this.getPrivateKeyFile()).as(String.class).orElseThrow());
            properties.put("private_key_file_pwd", runContext.render(this.getPrivateKeyFilePassword()).as(String.class).orElseThrow());
        }
    }

    @Override
    default String getScheme() {
        return "jdbc:snowflake";
    }
}
