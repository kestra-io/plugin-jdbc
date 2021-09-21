package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.Locale;
import java.util.Properties;

public abstract class PostgresService {
    public static void handleSsl(Properties properties, RunContext runContext, PostgresConnectionInterface conn) throws IllegalVariableEvaluationException, IOException {
        if (conn.getSsl() != null && conn.getSsl()) {
            properties.put("ssl", "true");
        }

        if (conn.getSslMode() != null) {
            properties.put("sslmode", conn.getSslMode().name().toUpperCase(Locale.ROOT).replace("_", "-"));
        }

        if (conn.getSslRootCert() != null) {
            properties.put("sslrootcert", runContext.tempFile(runContext.render(conn.getSslRootCert()).getBytes(StandardCharsets.UTF_8), ".pem").toAbsolutePath().toString());
        }

        if (conn.getSslCert() != null) {
            properties.put("sslcert", runContext.tempFile(runContext.render(conn.getSslCert()).getBytes(StandardCharsets.UTF_8), ".pem").toAbsolutePath().toString());
        }

        if (conn.getSslKey() != null) {
            properties.put("sslkey", convertPrivateKey(runContext, conn.getSslKey(), conn.getSslKeyPassword()));
        }

        if (conn.getSslKeyPassword() != null) {
            properties.put("sslpassword", runContext.render(conn.getSslKeyPassword()));
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

    private static String convertPrivateKey(RunContext runContext, String vars, String password) throws IOException, IllegalVariableEvaluationException {
        Provider bc = Security.getProvider("BC");
        if (bc == null) {
            Security.addProvider(new BouncyCastleProvider());
        }

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
        } else {
            keyInfo = ((PEMKeyPair) pemObject).getPrivateKeyInfo();
        }

        PrivateKey privateKey = new JcaPEMKeyConverter().getPrivateKey(keyInfo);

        return runContext.tempFile(privateKey.getEncoded(), ".der").toAbsolutePath().toString();
    }
}
