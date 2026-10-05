package io.kestra.plugin.jdbc.oracle;

import oracle.sql.CharacterSet;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class OracleCharsetTest {
    // ZHS16GBK (Simplified Chinese) is not built into ojdbc: it needs orai18n on the classpath,
    // otherwise any conversion fails with ORA-17056.
    @Test
    void shouldSupportZhs16gbkCharacterSet() throws Exception {
        CharacterSet charset = CharacterSet.make(CharacterSet.ZHS16GBK_CHARSET);
        String text = "\u4e2d\u6587"; // "中文" (Chinese)

        byte[] bytes = charset.convertWithReplacement(text);

        assertThat(charset.toString(bytes, 0, bytes.length), is(text));
    }
}
