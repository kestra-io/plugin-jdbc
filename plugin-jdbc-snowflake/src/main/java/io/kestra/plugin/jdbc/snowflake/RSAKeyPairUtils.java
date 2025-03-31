package io.kestra.plugin.jdbc.snowflake;

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

public class RSAKeyPairUtils {
    public static PrivateKey deserializePrivateKey(String privateKey, Optional<String> privateKeyPassword) {
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
}
