package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import java.io.IOException;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Optional;
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
        title = "Specifies the private key password for key pair authentication and key rotation.")
    Property<String> getPrivateKeyPassword();

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

            var unencryptedPrivateKey = deserializePrivateKey(
                runContext.render(this.getPrivateKey()).as(String.class).orElseThrow(),
                runContext.render(this.getPrivateKeyPassword()).as(String.class)
            );
            properties.put("privateKey", unencryptedPrivateKey);
        }

        if (this.getPrivateKeyFile() != null && this.getPrivateKeyFilePassword() != null) {
            properties.put("private_key_file", runContext.render(this.getPrivateKeyFile()).as(String.class).orElseThrow());
            properties.put("private_key_file_pwd", runContext.render(this.getPrivateKeyFilePassword()).as(String.class).orElseThrow());
        }

    }

    private PrivateKey deserializePrivateKey(String privateKey, Optional<String> privateKeyPassword) {
        Security.addProvider(new BouncyCastleProvider());
        var keyBytes = Base64.getDecoder().decode(privateKey);
        try {
            // Try unencrypted first
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
            return keyFactory.generatePrivate(keySpec);
        } catch (IllegalArgumentException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            // Try encrypted
            var password = privateKeyPassword.orElseThrow(() -> new IllegalArgumentException("Private key seems to be encrypted, password is required"));
            return deserializeEncryptedPrivateKey(keyBytes, password);
        }
    }

    private static PrivateKey deserializeEncryptedPrivateKey(byte[] keyBytes, String password) {
        PrivateKey unencryptedPrivateKey;
        try {
            PKCS8EncryptedPrivateKeyInfo encryptedInfo = new PKCS8EncryptedPrivateKeyInfo(keyBytes);
            InputDecryptorProvider decryptorProvider = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(password.toCharArray());
            PrivateKeyInfo privateKeyInfo = encryptedInfo.decryptPrivateKeyInfo(decryptorProvider);

            KeyFactory keyFactory = KeyFactory.getInstance(privateKeyInfo.getPrivateKeyAlgorithm().getAlgorithm().getId(), "BC");
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyInfo.getEncoded());
            unencryptedPrivateKey = keyFactory.generatePrivate(keySpec);
        } catch (IOException | NoSuchProviderException | OperatorCreationException | PKCSException |
                 NoSuchAlgorithmException | InvalidKeySpecException e2) {
            throw new RuntimeException("Could not read private key: " + e2.getMessage(), e2);
        }
        return unencryptedPrivateKey;
    }

    @Override
    default String getScheme() {
        return "jdbc:snowflake";
    }
}
