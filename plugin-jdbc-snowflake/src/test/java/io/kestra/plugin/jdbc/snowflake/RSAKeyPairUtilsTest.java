package io.kestra.plugin.jdbc.snowflake;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RSAKeyPairUtilsTest {

    public static final String UNENCRYPTED_PRIVATE_KEY = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQClsS+74Y9wnwbwAsFcvAHMqU4G35HVEgkNkI0Xel/5o8E0WA2VW6bGEw0fbQrKgngNRque5ZWy3S26YYBt2IEYBvc0FJl3MkoHYcgFsNhFCU1Ik+hRo2YlOv4l6YcK4k18MRnKrl92q74wMb1VMz5vDI52httenxCmAzp9woE4UsWS9vgKublUp1ExrLAVr7yj038mogwVTZdEZoWd9HP4TTUVGBr5C0c0+vt9uEuticDlEUDdR2zvFxVsk91n/jmXk4k0hCPWHG0IPvEzS6XjLJeU0rLubv2sU2tMUUWqoSYmJKV+PlHgf/ERL/zGIqQF0puJbTJLqDMYYkvUFE0vAgMBAAECggEAeAMf7PkSuWMmVj/YqH+w2fmjf4z+BxO6JO4Xk/Lag2od7fj9Vbp90KhJ8AI+N7JKnGscscnfJR/ZGE+5A1c3Ih0hfsKQ6eot/qzPgXe3HkH/jVs8ga1Vtg/Ft9YvLy39K8Awy0KD+OOqrSPJ3GVyimLQ6X8Cc8XI/EYIXsC8cfsvjjfPHHdmdRdiAHCdIsPa1t84WuTIYPl8ZaEO3eITtCxUJyBAdm+lJhHmkVpl7jLadwG2JSIE8ptYXyuZxClHe87wHWhxspHw6nIXi6wUca3OS2GylQE0KsxXt20ujCZ+J4yfLcCs9gCsk8jdgUqw6cz277ZsHoNIN2ksh8QiwQKBgQDUI7ePI8o+lDhB91VpEdfvYcwERvkoTsAUr79vf3jBmZdJ/BlUQopbihp6jinjkmzScPP5SpCupCmS8Wdy0GHiNydXwiHyunWImnvrN1Stn/KqO2SP39Y0aN06DSJyHp0+6yg6EZQDkFjFRhqDzEZF6Nx1r7dqz/b+CbdE7aCNTwKBgQDH8xEQ5dJeWBoSH/ikuLwcZArhuQuO+NqcmIngEiDOM6yin8e16Pca2NBwnBxVIVDtV+cVpqf/mpYKdRGbr5k3HYPPOk3gN+smF310LNaKhDimLrDX04zS12psDaH0xBdkUhc+74brK4f6QZTscc4pUCWml/03OfGulEsmkBMKIQKBgQCJjp94Mbzy1ymjnL3FY8yhbMjd/SeS+32R7GQ16HJlFzMCxnWmLX+J3ANPdN+0sT3fN178R12J4OkUX3S0/mp6RUk9nqRkwIN2ELZJz5cY/p5bxCdxI7PCSD0rZ1juputP4Sw0ezF/Hhpx0QNgcxUeP+C0bzyABloiYCp2h5+UAQKBgBKTwc0pHS+IzftLHyXmyAt3PA+Wyr0g3+bWwVChPo0J/gsp/zsmhvbmmA8uYe+C5er3VNANKOS1ryUPlXA6k2ishFeXmi6v41gprI1DsTcza7P8+I9E53ubahbZ+WctZOL8QY6mYImvBLY6q6SAOmBaQvVEf9FGv+6xdn4ButMhAoGABn+UUkaayq6gLo0QIINzhgpB/qegVLu/k6bEcOC/09FZRmErfqszXv3SDAvxqcwVncDauFRyDcYm3w+or5NiKDplZcEs0OTp6iK7jTplpU/4DXtVr6quEbX+Xu2+T0RFnJbUzKaX9oD/0AgW+pQ1gGY9JUN6Mns6yJLlmLjRloA=";
    // this following key was manny encrypted with following password
    public static final String ENCRYPTED_PRIVATE_KEY = "MIIFDjBABgkqhkiG9w0BBQ0wMzAbBgkqhkiG9w0BBQwwDgQIPGfqlvK891MCAggAMBQGCCqGSIb3DQMHBAg+PXQ73CNFwQSCBMhxYfdU+4IS0EYxOfC3t2Xa6f6shelDZEHvd7B/tlKrW4fy3W8BKv8obXX2RO2XrF/NHXEbG0gYLdvQNOMMov4UyGQXLh24U3yBNSDij0CwM/J9FtqU+oWvgrzQtXzxGO4qvzwDiwiKdU2AUkKvJOXqe+A/PAOXkNHsWxqkKHv3n50y/LlpLIGMdSkAf5wMFifqudyegEeNqM5d3k0ExCj62IUl7wjn380RcVXJ73iWubGREetRTMt466ShQaf/YYvSFSkHdb8GzIpdepBw6Ny23nuyRnv0LciJcQQe2fwt9u6JcNTMBz4+2IKLn75JvsII5ho9ROT3zdmcpt9aF/i7ZH1Plbr1ZtU04jioONcXB7tUAfTENioGtDPvo62gpsH/ekElD5H+9wDrdr2+yavGDsKFVqloMJau8pLSHXTYZsj/IzcnH+1OtpYvpb3xGsH9GFr9/RWP4IopfN4SnEE9D+imQbdtO/UpMa3ixM0XzYgFgULAbOSZRDgCplmGzLc4bolu7hDxuVnDnswZnTJ23Cmu4WTbGd0ORsgOm/HIqkeiGeRM8jjAf7OkDI7m/ybbUwc36ZEwUWdIzGW63bdQWALdrOObenRBBkIaoAkitmtL4OhVDVqxvNmKyvNiC5rXWGp+TXabpv0VDn6HTPpFU9L2OxDlQUsupO+kBwl+wsMdIK/M7AlNz3IwyzpQMrXDN7WnemevpnKh0Do9VavVm8CGXZFVMm2dPjLbfOC7t0fUPs/8/iy+364qYsV3NGD8rAQKBL7pPr89sqxSUOwtcGCHUcKH8qJ14W1+oNhQaPKU/onyrFXUdFJK1rOfn7U51WVp3+RDY2j+MBQnTOtCGxZG65Fo6jRo4hA2uAqOmPZm7znHqmM16rud+tmbRe3JbXf1J4j/O0Cu0ucXo+apOSGKn8uMWGUwJUR+tH0yNnAgbeU4DkB/RQOUZKLzML8CRtRvH/tzhfpzOx2eVVAog+TooCp1cXh85/UTkYX9VU4f++pNcG2JmZMSuulTC9Om98w7Hby7AX19qyMYOrc8aIupOgoPjKG1IMHhyIeJs73wSARXM+VCbFmoG9IsEd7e6CIkKiJLLegtaLN1SZ2XkKRl2Z6y0Mkv7LRpApmo41tJ8dEi2R6amstAJCDnS8wzWscLyXgdm3xrQxD6Ctb3fbAJ004HUQWxjfD76v1ofvR6DUtbjWd9soB4nsCjaTHsyXS1K4dBF98kA62wxKct2ezIWUDS/Nfi797r6P/Doc9cuHshl7JsDpj6dGEZf4ykjBD+EVXLKwUWR6k+IQ1nGP7KxN96zCsfxo93cJ1oNeCawHl085elhoxMbm9TJqGE4h7BkGm3pvKiKMEkTmf7CV7NSpGXXWVYFCw3J/2kKME8119KZtSf5LAHdl4ZnCaXAWaHQmpei1oNkquDAWvwxyapdJCCDsXAxEB+nxhFabiPCnBQHr5qGo8gs18i3bq9kX69e2HFn+4xHG0U1rKbJ9KZ7OECVAswX21UlN9vMzEcYhTmu6CVl9Iq4WAJ5XgEA7oIkEhD3cg5ciXzxrswBGuKQcPkfiPoOMEZfTqJQT/yVGWngZguNph139Usyrq5LXVVsoeldMPIO+LXL22iQ+ML8K5V7XA=";
    public static final String ENCRYPTED_PRIVATE_KEY_PASSWORD = "mypassword";

    @Test
    void canDeserializeAnUnencryptedPrivateKey() {
        var res = RSAKeyPairUtils.deserializePrivateKey(
            UNENCRYPTED_PRIVATE_KEY,
            Optional.empty()
        );
        assertThat(res.getFormat(), is("PKCS#8"));
        assertThat(res.getAlgorithm(), is("RSA"));
    }

    @Test
    void passwordIsRequiredForEncryptedPrivateKey() {
        var raisedException = assertThrows(RuntimeException.class, () ->
            RSAKeyPairUtils.deserializePrivateKey(
                ENCRYPTED_PRIVATE_KEY,
                Optional.empty()
            ));
        assertThat(raisedException.getMessage(), containsString("Could not read private key"));
    }

    @Test
    void canDeserializeEncryptedPrivateKey() {
        var res = RSAKeyPairUtils.deserializePrivateKey(
            ENCRYPTED_PRIVATE_KEY,
            Optional.of(ENCRYPTED_PRIVATE_KEY_PASSWORD)
        );
        assertThat(res.getFormat(), is("PKCS#8"));
        assertThat(res.getAlgorithm(), is("RSA"));
    }

    @Test
    void encryptedKeyShouldBeEqualsToUnecryptedKey() {
        var deserializedEncryptedKey = RSAKeyPairUtils.deserializePrivateKey(
            ENCRYPTED_PRIVATE_KEY,
            Optional.of(ENCRYPTED_PRIVATE_KEY_PASSWORD)
        );
        var deserializedUnencryptedKey = RSAKeyPairUtils.deserializePrivateKey(
            UNENCRYPTED_PRIVATE_KEY,
            Optional.empty()
        );
        assertThat(deserializedEncryptedKey.getEncoded(), is(deserializedUnencryptedKey.getEncoded()));
    }
}
