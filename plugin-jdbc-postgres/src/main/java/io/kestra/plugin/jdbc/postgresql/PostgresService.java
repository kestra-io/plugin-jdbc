package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.BouncyGPG;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JceInputDecryptorProviderBuilder;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.util.Locale;
import java.util.Properties;

public abstract class PostgresService {
    public static void handleSsl(Properties properties, RunContext runContext, PostgresConnectionInterface conn) throws Exception {
        if (conn.getSsl() != null && runContext.render(conn.getSsl()).as(Boolean.class).orElseThrow()) {
            properties.put("ssl", "true");
        }

        if (conn.getSslMode() != null) {
            properties.put("sslmode", runContext.render(conn.getSslMode()).as(PostgresConnectionInterface.SslMode.class).orElseThrow().name().toUpperCase(Locale.ROOT).replace("_", "-"));
        }

        if (conn.getSslRootCert() != null) {
            properties.put("sslrootcert", runContext.workingDir().createTempFile(runContext.render(conn.getSslRootCert()).as(String.class).orElseThrow().getBytes(StandardCharsets.UTF_8), ".pem").toAbsolutePath().toString());
        }

        if (conn.getSslCert() != null) {
            properties.put("sslcert", runContext.workingDir().createTempFile(runContext.render(conn.getSslCert()).as(String.class).orElseThrow().getBytes(StandardCharsets.UTF_8), ".pem").toAbsolutePath().toString());
        }

        if (conn.getSslKey() != null) {
            properties.put("sslkey", convertPrivateKey(runContext, runContext.render(conn.getSslKey()).as(String.class).orElse(null),
                runContext.render(conn.getSslKeyPassword()).as(String.class).orElse(null)));
        }

        if (conn.getSslKeyPassword() != null) {
            properties.put("sslpassword", runContext.render(conn.getSslKeyPassword()).as(String.class).orElseThrow());
        }
    }

    private static Object readPem(RunContext runContext, String vars) throws IllegalVariableEvaluationException, IOException {
        try (
            StringReader reader = new StringReader(runContext.render(vars));
            PEMParser pemParser = new PEMParser(reader)
        ) {
            return pemParser.readObject();
        }
    }

    private static synchronized void addProvider() {
        Provider bc = Security.getProvider("BC");
        if (bc == null) {
            BouncyGPG.registerProvider();
        }
    }

    private static String convertPrivateKey(RunContext runContext, String vars, String password) throws IOException, IllegalVariableEvaluationException, PKCSException, OperatorCreationException {
        PostgresService.addProvider();

        Object pemObject = readPem(runContext, vars);

        PrivateKeyInfo keyInfo;
        if (pemObject instanceof PEMEncryptedKeyPair) {
            if (password == null) {
                throw new IOException("Unable to import private key. Key is encrypted, but no password was provided.");
            }

            PEMDecryptorProvider decrypter = new JcePEMDecryptorProviderBuilder()
                .setProvider("BC")
                .build(password.toCharArray());

            PEMKeyPair decryptedKeyPair = ((PEMEncryptedKeyPair) pemObject).decryptKeyPair(decrypter);
            keyInfo = decryptedKeyPair.getPrivateKeyInfo();
        } else if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
            if (password == null) {
                throw new IOException("Unable to import private key. Key is encrypted, but no password was provided.");
            }

            InputDecryptorProvider inputDecryptorProvider = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                .setProvider("BC")
                .build(password.toCharArray());

            keyInfo = ((PKCS8EncryptedPrivateKeyInfo) pemObject).decryptPrivateKeyInfo(inputDecryptorProvider);
        } else if (pemObject instanceof PrivateKeyInfo) {
            keyInfo = ((PrivateKeyInfo) pemObject);
        } else {
            keyInfo = ((PEMKeyPair) pemObject).getPrivateKeyInfo();
        }

        PrivateKey privateKey = new JcaPEMKeyConverter().getPrivateKey(keyInfo);

        return runContext.workingDir().createTempFile(privateKey.getEncoded(), ".der").toAbsolutePath().toString();
    }
}
