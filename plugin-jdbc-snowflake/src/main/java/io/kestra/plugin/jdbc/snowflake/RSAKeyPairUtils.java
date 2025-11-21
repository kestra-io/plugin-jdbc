package io.kestra.plugin.jdbc.snowflake;

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.pkcs.RSAPrivateKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

import java.io.StringReader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateKeySpec;
import java.util.Base64;
import java.util.Optional;

@Slf4j
public class RSAKeyPairUtils {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    /**
     * Accepts multiple formats:
     * - raw Base64 PKCS8 DER
     * - PEM PKCS8 ("BEGIN PRIVATE KEY")
     * - PEM PKCS1 RSA ("BEGIN RSA PRIVATE KEY")
     * - encrypted PKCS8 (if password provided)
     * - multiline keys
     */
    public static PrivateKey deserializePrivateKey(String privateKey, Optional<String> privateKeyPassword) {
        try {
            if (privateKey.contains("BEGIN")) {
                log.info("Trying to parse private key as PEM format");
                return parsePem(privateKey, privateKeyPassword);
            }

            byte[] keyBytes = Base64.getMimeDecoder().decode(privateKey);

            String decodedAsString = new String(keyBytes);
            if (decodedAsString.contains("BEGIN")) {
                log.info("Base64 decoded to PEM, trying PEM parser");
                return parsePem(decodedAsString, privateKeyPassword);
            }

            try {
                log.info("Trying to parse private key as PKCS8 DER format");
                return generatePkcs8PrivateKey(keyBytes);
            } catch (InvalidKeySpecException e) {
                if (isPkcs8EncryptedDer(keyBytes)) {
                    if (privateKeyPassword.isEmpty()) {
                        throw new IllegalArgumentException("Private key is encrypted but no password provided");
                    }
                    return decryptPkcs8PrivateKey(keyBytes, privateKeyPassword.get());
                }
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not read private key: " + e.getMessage(), e);
        }
    }

    private static PrivateKey parsePem(String pem, Optional<String> privateKeyPassword) throws Exception {
        try (PEMParser parser = new PEMParser(new StringReader(pem))) {
            Object obj = parser.readObject();

            // Encrypted PKCS8
            if (obj instanceof PKCS8EncryptedPrivateKeyInfo encryptedInfo) {
                if (privateKeyPassword.isEmpty()) {
                    throw new IllegalArgumentException("Private key is encrypted but no password provided");
                }
                InputDecryptorProvider decProv =
                    new JceOpenSSLPKCS8DecryptorProviderBuilder()
                        .build(privateKeyPassword.get().toCharArray());
                PrivateKeyInfo pkInfo = encryptedInfo.decryptPrivateKeyInfo(decProv);
                return new JcaPEMKeyConverter().getPrivateKey(pkInfo);
            }

            // PKCS#1 PEM as PEMKeyPair
            if (obj instanceof PEMKeyPair pemKeyPair) {
                PrivateKeyInfo pkInfo = pemKeyPair.getPrivateKeyInfo();
                return new JcaPEMKeyConverter().getPrivateKey(pkInfo);
            }

            // PKCS#1 raw ASN.1 sequence
            if (obj instanceof ASN1Sequence sequence) {
                return convertPkcs1ToPrivateKey(sequence);
            }

            // Unencrypted PKCS8
            if (obj instanceof PrivateKeyInfo pkInfo) {
                return new JcaPEMKeyConverter().getPrivateKey(pkInfo);
            }

            throw new IllegalArgumentException("Unknown PEM private key format");
        }
    }

    /**
     * Converts PKCS1 RSA (ASN1 sequence) => PKCS8 PrivateKey
     */
    private static PrivateKey convertPkcs1ToPrivateKey(ASN1Sequence seq)
        throws NoSuchAlgorithmException, InvalidKeySpecException {

        RSAPrivateKey rsaPrivateKey = RSAPrivateKey.getInstance(seq);

        RSAPrivateKeySpec keySpec = new RSAPrivateKeySpec(
            rsaPrivateKey.getModulus(),
            rsaPrivateKey.getPrivateExponent()
        );

        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(keySpec);
    }

    private static PrivateKey generatePkcs8PrivateKey(byte[] keyBytes) throws NoSuchAlgorithmException, InvalidKeySpecException {
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        return KeyFactory.getInstance("RSA").generatePrivate(spec);
    }

    private static boolean isPkcs8EncryptedDer(byte[] keyBytes) {
        try {
            new PKCS8EncryptedPrivateKeyInfo(keyBytes);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static PrivateKey decryptPkcs8PrivateKey(byte[] keyBytes, String password) throws Exception {
        PKCS8EncryptedPrivateKeyInfo encryptedInfo = new PKCS8EncryptedPrivateKeyInfo(keyBytes);
        InputDecryptorProvider decProv = new JceOpenSSLPKCS8DecryptorProviderBuilder()
            .build(password.toCharArray());
        PrivateKeyInfo pkInfo = encryptedInfo.decryptPrivateKeyInfo(decProv);
        return new JcaPEMKeyConverter().getPrivateKey(pkInfo);
    }
}
