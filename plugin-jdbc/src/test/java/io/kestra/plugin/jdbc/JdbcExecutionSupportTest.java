package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class JdbcExecutionSupportTest {
    @Test
    void longerSecretsAreMaskedFirst() {
        assertThat(JdbcExecutionSupport.mask("SELECT 'password', 'pass'", List.of("pass", "password")), is("SELECT '***', '***'"));
    }

    @Test
    void base64FormsAreMasked() {
        var encoded = Base64.getEncoder().encodeToString("s3cr3t-value".getBytes(StandardCharsets.UTF_8));

        assertThat(JdbcExecutionSupport.mask("SELECT '" + encoded + "', 's3cr3t-value'", List.of("s3cr3t-value")), is("SELECT '***', '***'"));
    }

    @Test
    void emptyAndNullSecretsAreIgnored() {
        assertThat(JdbcExecutionSupport.mask("SELECT 1", java.util.Arrays.asList("", null)), is("SELECT 1"));
    }
}
