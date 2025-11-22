package io.kestra.plugin.jdbc.snowflake;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RSAKeyPairUtilsTest {

    // CASES WITHOUT NO HEADERS "-----BEGIN PRIVATE KEY-----"
    public static final String UNENCRYPTED_PRIVATE_KEY_NO_HEADERS = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQClsS+74Y9wnwbwAsFcvAHMqU4G35HVEgkNkI0Xel/5o8E0WA2VW6bGEw0fbQrKgngNRque5ZWy3S26YYBt2IEYBvc0FJl3MkoHYcgFsNhFCU1Ik+hRo2YlOv4l6YcK4k18MRnKrl92q74wMb1VMz5vDI52httenxCmAzp9woE4UsWS9vgKublUp1ExrLAVr7yj038mogwVTZdEZoWd9HP4TTUVGBr5C0c0+vt9uEuticDlEUDdR2zvFxVsk91n/jmXk4k0hCPWHG0IPvEzS6XjLJeU0rLubv2sU2tMUUWqoSYmJKV+PlHgf/ERL/zGIqQF0puJbTJLqDMYYkvUFE0vAgMBAAECggEAeAMf7PkSuWMmVj/YqH+w2fmjf4z+BxO6JO4Xk/Lag2od7fj9Vbp90KhJ8AI+N7JKnGscscnfJR/ZGE+5A1c3Ih0hfsKQ6eot/qzPgXe3HkH/jVs8ga1Vtg/Ft9YvLy39K8Awy0KD+OOqrSPJ3GVyimLQ6X8Cc8XI/EYIXsC8cfsvjjfPHHdmdRdiAHCdIsPa1t84WuTIYPl8ZaEO3eITtCxUJyBAdm+lJhHmkVpl7jLadwG2JSIE8ptYXyuZxClHe87wHWhxspHw6nIXi6wUca3OS2GylQE0KsxXt20ujCZ+J4yfLcCs9gCsk8jdgUqw6cz277ZsHoNIN2ksh8QiwQKBgQDUI7ePI8o+lDhB91VpEdfvYcwERvkoTsAUr79vf3jBmZdJ/BlUQopbihp6jinjkmzScPP5SpCupCmS8Wdy0GHiNydXwiHyunWImnvrN1Stn/KqO2SP39Y0aN06DSJyHp0+6yg6EZQDkFjFRhqDzEZF6Nx1r7dqz/b+CbdE7aCNTwKBgQDH8xEQ5dJeWBoSH/ikuLwcZArhuQuO+NqcmIngEiDOM6yin8e16Pca2NBwnBxVIVDtV+cVpqf/mpYKdRGbr5k3HYPPOk3gN+smF310LNaKhDimLrDX04zS12psDaH0xBdkUhc+74brK4f6QZTscc4pUCWml/03OfGulEsmkBMKIQKBgQCJjp94Mbzy1ymjnL3FY8yhbMjd/SeS+32R7GQ16HJlFzMCxnWmLX+J3ANPdN+0sT3fN178R12J4OkUX3S0/mp6RUk9nqRkwIN2ELZJz5cY/p5bxCdxI7PCSD0rZ1juputP4Sw0ezF/Hhpx0QNgcxUeP+C0bzyABloiYCp2h5+UAQKBgBKTwc0pHS+IzftLHyXmyAt3PA+Wyr0g3+bWwVChPo0J/gsp/zsmhvbmmA8uYe+C5er3VNANKOS1ryUPlXA6k2ishFeXmi6v41gprI1DsTcza7P8+I9E53ubahbZ+WctZOL8QY6mYImvBLY6q6SAOmBaQvVEf9FGv+6xdn4ButMhAoGABn+UUkaayq6gLo0QIINzhgpB/qegVLu/k6bEcOC/09FZRmErfqszXv3SDAvxqcwVncDauFRyDcYm3w+or5NiKDplZcEs0OTp6iK7jTplpU/4DXtVr6quEbX+Xu2+T0RFnJbUzKaX9oD/0AgW+pQ1gGY9JUN6Mns6yJLlmLjRloA=";
    public static final String ENCRYPTED_PRIVATE_KEY_NO_HEADERS = "MIIFDjBABgkqhkiG9w0BBQ0wMzAbBgkqhkiG9w0BBQwwDgQIPGfqlvK891MCAggAMBQGCCqGSIb3DQMHBAg+PXQ73CNFwQSCBMhxYfdU+4IS0EYxOfC3t2Xa6f6shelDZEHvd7B/tlKrW4fy3W8BKv8obXX2RO2XrF/NHXEbG0gYLdvQNOMMov4UyGQXLh24U3yBNSDij0CwM/J9FtqU+oWvgrzQtXzxGO4qvzwDiwiKdU2AUkKvJOXqe+A/PAOXkNHsWxqkKHv3n50y/LlpLIGMdSkAf5wMFifqudyegEeNqM5d3k0ExCj62IUl7wjn380RcVXJ73iWubGREetRTMt466ShQaf/YYvSFSkHdb8GzIpdepBw6Ny23nuyRnv0LciJcQQe2fwt9u6JcNTMBz4+2IKLn75JvsII5ho9ROT3zdmcpt9aF/i7ZH1Plbr1ZtU04jioONcXB7tUAfTENioGtDPvo62gpsH/ekElD5H+9wDrdr2+yavGDsKFVqloMJau8pLSHXTYZsj/IzcnH+1OtpYvpb3xGsH9GFr9/RWP4IopfN4SnEE9D+imQbdtO/UpMa3ixM0XzYgFgULAbOSZRDgCplmGzLc4bolu7hDxuVnDnswZnTJ23Cmu4WTbGd0ORsgOm/HIqkeiGeRM8jjAf7OkDI7m/ybbUwc36ZEwUWdIzGW63bdQWALdrOObenRBBkIaoAkitmtL4OhVDVqxvNmKyvNiC5rXWGp+TXabpv0VDn6HTPpFU9L2OxDlQUsupO+kBwl+wsMdIK/M7AlNz3IwyzpQMrXDN7WnemevpnKh0Do9VavVm8CGXZFVMm2dPjLbfOC7t0fUPs/8/iy+364qYsV3NGD8rAQKBL7pPr89sqxSUOwtcGCHUcKH8qJ14W1+oNhQaPKU/onyrFXUdFJK1rOfn7U51WVp3+RDY2j+MBQnTOtCGxZG65Fo6jRo4hA2uAqOmPZm7znHqmM16rud+tmbRe3JbXf1J4j/O0Cu0ucXo+apOSGKn8uMWGUwJUR+tH0yNnAgbeU4DkB/RQOUZKLzML8CRtRvH/tzhfpzOx2eVVAog+TooCp1cXh85/UTkYX9VU4f++pNcG2JmZMSuulTC9Om98w7Hby7AX19qyMYOrc8aIupOgoPjKG1IMHhyIeJs73wSARXM+VCbFmoG9IsEd7e6CIkKiJLLegtaLN1SZ2XkKRl2Z6y0Mkv7LRpApmo41tJ8dEi2R6amstAJCDnS8wzWscLyXgdm3xrQxD6Ctb3fbAJ004HUQWxjfD76v1ofvR6DUtbjWd9soB4nsCjaTHsyXS1K4dBF98kA62wxKct2ezIWUDS/Nfi797r6P/Doc9cuHshl7JsDpj6dGEZf4ykjBD+EVXLKwUWR6k+IQ1nGP7KxN96zCsfxo93cJ1oNeCawHl085elhoxMbm9TJqGE4h7BkGm3pvKiKMEkTmf7CV7NSpGXXWVYFCw3J/2kKME8119KZtSf5LAHdl4ZnCaXAWaHQmpei1oNkquDAWvwxyapdJCCDsXAxEB+nxhFabiPCnBQHr5qGo8gs18i3bq9kX69e2HFn+4xHG0U1rKbJ9KZ7OECVAswX21UlN9vMzEcYhTmu6CVl9Iq4WAJ5XgEA7oIkEhD3cg5ciXzxrswBGuKQcPkfiPoOMEZfTqJQT/yVGWngZguNph139Usyrq5LXVVsoeldMPIO+LXL22iQ+ML8K5V7XA=";
    public static final String ENCRYPTED_PRIVATE_KEY_NO_HEADERS_PASSWORD = "mypassword";

    // UNENCRYPTED PKCS8 PEM
    public static final String PEM_PKCS8 = """
        -----BEGIN PRIVATE KEY-----
        MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAOVEs9HsPNnnf8ig
        YWL6wAUvN5jnwvb6gtwCzely8b4ojMdlxeTHe2X1xDnBB15xm9149WEujyhbufTY
        eo4Rt74VDouaw7e/SW+LtuGeQ7zuGUIxYriXZ6Q9lqx+U5NE+TjkCvbLp4xMfgss
        4+gt/J1d4GM+A/VvsU2qMyWI4vqBAgMBAAECgYEAxDA7NErUc58PEQ50568tPAKA
        r/67Ln+GFWDs9XTf+tpWRZcIddJh/QkHJmjQtne/ahDE4alm5aFAio3oqcPtl0No
        iEAUT/sG3s8Th3o4IWdMB2TqWGF0ZMOM80tgyPs+X0R+s0dvsxgk1oKlGqVpgsMq
        aEvZOJkgr2mdBXy9scECQQDz7AWIzdWPv2wGaU3LQc5SoFi4z5Q56KgPecMegMdM
        UpweMgCwgkUXQTrMqhZRHvAxTDKPwS4EcUgLM4/zO4e5AkEA8J7uMz6frwtlNnng
        ZVI196AC2gfKToLABBcn9OIE0SLWVNKTgVMkonqM3U3I9YBnD1TNBlrlwVPlDePE
        C+VdCQJBAIhma5HcyJfhy16qdD49RkseL37pVVIssA43YM0l5kzfxT19aLVLo6cl
        auQYGPK0Ak0O9xc8R6dkUY0yAEVb/MECQQC+q3HdsZYfw0vkqxchss+I9YbM9rdd
        F0bI9wB2kFN41b45YNP5+sRg6/OLugOwZptEDtKYYpcFZ1FufDnxo0LhAkBS+UM2
        MtqViOwvdqAk6WdHrZ3YAbLBFdyvn+b6APTK3Xb7fdcjvfhW9AvwjdkXI7fc3gyf
        FqFQA2QEcip6QqVu
        -----END PRIVATE KEY-----
        """;

    // ENCRYPTED PKCS8 (AES-256)
    public static final String ENCRYPTED_PRIVATE_KEY = """
        -----BEGIN ENCRYPTED PRIVATE KEY-----
        MIIC5TBfBgkqhkiG9w0BBQ0wUjAxBgkqhkiG9w0BBQwwJAQQ22U9bOUx5DmLcPsw
        J/Q3hQICCAAwDAYIKoZIhvcNAgkFADAdBglghkgBZQMEASoEEBKioGK1reHZ8QjN
        t2FX0jkEggKAzZFb6NlhB1FcuiO9Q/l+OPXEeBjGJhW0t4gBcoLHQkAght3hOY+T
        8c3Tb+W43GOY3vr3W5oxHaZPwhzK/tH0lyZlkXnqD5YHs8t91ozS5J/sZDe/zPGt
        xP1Ui+UveAviaUeWDEys8T4aYmiygqDhnexMF9hPwWYUcBUDXVbJlbINAL9XR6it
        CFPof4I/shLFesRh7Ds3u3A7W7cuiuZNEJOS2TCn08tkcRtWfOQtWC61zh1M6iX0
        eMILjgpiKdPYb/iw+qat3rtGLMJZXhVyzU1yy80Qi4lZt46s49PDz4EywLy2dH+q
        Sx2/upSVp9il/cDs37IV5pgMu0P8I5KRRfVlPOct+3tgDmkVP33knnha+gK4aXZh
        HhBWcJbjPC32rFuHxG9LuNBCNjNs9GS+PvLlzhqfEXjlWufzc4FPpC+FsJlf8f5t
        t41QVbKCwQzgj9XGk4VCmB5/Qpghi2iUmbJK+XewQVK/vWQYSKfCEYWBLu+BpYy7
        uBA4DiXAQm7hlp3R7y2qQq7ruREQp3n7vQ6Cktn8h019NxQ2uLpUxXWfQdin8Nxi
        vA+KOv9JuF+1R+TsSWl6PM1rHsT3Jg7QS/VaJO2+IDiskIyvB/t7FqYG3ONKls6j
        0wALN83GB5ioUfGkI1AEWxR/6Ih5Qy4pHHncxw6JdCUnLTcuIP82xZ8lNqBMza9U
        YLG6WIpRvmOY92LzqIZBSH1VzTJz3IuAEw2j6fAK2Pgp9Hjywy2h2HPIFIX9HoTm
        VLNktlNK4WM1HK4PF+38bpw8+0UgoF+TZDcJOK6VLrTAtw3gJIpp47sq/kL2emTa
        DRNw0UkNM4iC1K0NBYImTfDmK8HKOef+jA==
        -----END ENCRYPTED PRIVATE KEY-----
        """;

    public static final String ENCRYPTED_PRIVATE_KEY_PASSWORD = "kestra-test";

    public static final String PEM_PKCS1 = """
        -----BEGIN RSA PRIVATE KEY-----
        MIICXAIBAAKBgQDLZfFqm4kOqjE7H54vYH+8sv5dIgxqii+FkU6ZUBdPKduI8YbG
        yXBMQ9D7ykEbZV1fLHCz4hbdzTU5r76ocgQhP/RpynVn3NfroF+xW9tHThkfB/LO
        682otChLRj6Zbpl7hDsAeXnlRLFi0gci92LNDYxf48tkmAt3SBWwS0FlFwIDAQAB
        AoGAR6BG9CU6Nu5GgtnzDKrLypNVE+L9PlZwe+t0jKJ6m4QfN6E3pfxb01hvGvLj
        pQNudkZGPv8qYWGN64Qu7T3HL5KfKrXQldnNxXPpWorGNwzi+jXA6s4LGQMtxZfc
        ojbqOlqZ0WhZ83ZgjByOxrbsEFrltN95u2EfPuNZWVJ41AECQQD+g7tUxuaHSslu
        Mp+G7VZ4NNYQBytBIX9vceGYiEaaVYNECcDfOqCq0quvVtViq1wvRZOcA9Z1LfL8
        CcJ1fLTBAkEAyVfLg5GzycTuHdI4qp4SeEEtMLXJK/IBP36m+SywwmUrqWRMK4YE
        FiDF0R5TvZq/RrSoaQ5Z8E04kUdnzJqucQJBAKEyyZlvkkccFbadWVSficUZ8uho
        u/9AWKf5jscqR2gLwYANyqaKK+t/QtPq5uSTzBxk7SCFLTw4LyGzUvyhGTECQGx7
        LCUjPsnZXoo/luNT+HckcwgQ5MMGfrK1E+0PqnZW8HbE8O8eUwGK9vrWoXQOfohJ
        hcRT8yn2EuqrQgO0zPECQBYNHWnKbL0wkHqouGcWEfxWot1twj5xezTsBDkLhS/N
        FoWLEonx4MqEMI9Zv4bmR7NrVlP9adhbSbNSw3zfYVU=
        -----END RSA PRIVATE KEY-----
        """;

    public static final String PEM_PKCS1_ENCRYPTED = """
        -----BEGIN RSA PRIVATE KEY-----
        Proc-Type: 4,ENCRYPTED
        DEK-Info: AES-128-CBC,9A98E2CCE2D94C6FD67A746377BCF7C3

        C1xvTTAguYyI1xzwumJ9Y4AjcN9JCrZlnNbX2qZir4UueusfOqqiQ61rrShWftWw
        0R1cG9meBV52pFJLAvpkszb7l184N2kYY1p5B+sqvdMJIOWoSl5yIfhtUF3hZYtn
        zv3wzKmNMnFHZ2kNr5nq+rEIQWoF5v2OAv11nUrT8EE=
        -----END RSA PRIVATE KEY-----
        """;

    public static final String UNKNOWN_PEM = """
        -----BEGIN SUPER SECRET KEY-----
        AAAABBBBCCCCDDDD11112222
        -----END SUPER SECRET KEY-----
        """;

    @Test
    void canDeserializeAnUnencryptedPrivateKey() {
        var res = RSAKeyPairUtils.deserializePrivateKey(
            UNENCRYPTED_PRIVATE_KEY_NO_HEADERS,
            Optional.empty()
        );
        assertThat(res.getFormat(), is("PKCS#8"));
        assertThat(res.getAlgorithm(), is("RSA"));
    }

    @Test
    void encryptedKeyShouldBeEqualsToUnecryptedKey() {
        var deserializedEncryptedKey = RSAKeyPairUtils.deserializePrivateKey(
            ENCRYPTED_PRIVATE_KEY_NO_HEADERS,
            Optional.of(ENCRYPTED_PRIVATE_KEY_NO_HEADERS_PASSWORD)
        );
        var deserializedUnencryptedKey = RSAKeyPairUtils.deserializePrivateKey(
            UNENCRYPTED_PRIVATE_KEY_NO_HEADERS,
            Optional.empty()
        );
        assertThat(deserializedEncryptedKey.getEncoded(), is(deserializedUnencryptedKey.getEncoded()));
    }

    @Test
    void canDeserializeEncryptedPrivateKey() {
        var res = RSAKeyPairUtils.deserializePrivateKey(
            ENCRYPTED_PRIVATE_KEY_NO_HEADERS,
            Optional.of(ENCRYPTED_PRIVATE_KEY_NO_HEADERS_PASSWORD)
        );
        assertThat(res.getFormat(), is("PKCS#8"));
        assertThat(res.getAlgorithm(), is("RSA"));
    }

    @Test
    void passwordIsRequiredForEncryptedPrivateKey() {
        var raisedException = assertThrows(RuntimeException.class, () ->
            RSAKeyPairUtils.deserializePrivateKey(
                ENCRYPTED_PRIVATE_KEY_NO_HEADERS,
                Optional.empty()
            ));
        assertThat(raisedException.getMessage(), containsString("Could not read private key"));
    }

    @Test
    void canDeserializePkcs8Pem() {
        var key = RSAKeyPairUtils.deserializePrivateKey(PEM_PKCS8, Optional.empty());
        assertThat(key.getAlgorithm(), is("RSA"));
    }

    @Test
    void passwordRequiredForEncrypted() {
        assertThrows(RuntimeException.class, () ->
            RSAKeyPairUtils.deserializePrivateKey(ENCRYPTED_PRIVATE_KEY, Optional.empty())
        );
    }

    @Test
    void canDeserializeEncrypted() {
        var key = RSAKeyPairUtils.deserializePrivateKey(
            ENCRYPTED_PRIVATE_KEY,
            Optional.of(ENCRYPTED_PRIVATE_KEY_PASSWORD)
        );
        assertThat(key.getAlgorithm(), is("RSA"));
    }

    @Test
    void invalidFormatFails() {
        assertThrows(RuntimeException.class, () ->
            RSAKeyPairUtils.deserializePrivateKey("notakey", Optional.empty())
        );
    }

    @Test
    void canDeserializePkcs1Pem() {
        var key = RSAKeyPairUtils.deserializePrivateKey(PEM_PKCS1, Optional.empty());
        assertThat(key.getAlgorithm(), is("RSA"));
    }

    @Test
    void encryptedPkcs1Fails() {
        assertThrows(RuntimeException.class, () ->
            RSAKeyPairUtils.deserializePrivateKey(PEM_PKCS1_ENCRYPTED, Optional.of("password"))
        );
    }

    @Test
    void unknownPemFormatFails() {
        assertThrows(RuntimeException.class, () ->
            RSAKeyPairUtils.deserializePrivateKey(UNKNOWN_PEM, Optional.empty())
        );
    }
}
