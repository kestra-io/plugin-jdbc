package io.kestra.plugin.jdbc.postgresql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import jakarta.inject.Inject;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static io.kestra.core.models.tasks.common.FetchType.NONE;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class CopyTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void copy() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        CopyOut copyOut = CopyOut.builder()
                .url(Property.ofValue(TestUtils.url()))
                .username(Property.ofValue(TestUtils.username()))
                .password(Property.ofValue(TestUtils.password()))
                .format(Property.ofValue(AbstractCopy.Format.CSV))
                .header(Property.ofValue(true))
                .delimiter(Property.ofValue('\t'))
                .escape(Property.ofValue('"'))
                .forceQuote(Property.ofValue(Collections.singletonList("int")))
                .sql(Property.ofValue("SELECT 1 AS int, 't'::bool AS bool UNION SELECT 2 AS int, 'f'::bool AS bool "))
                .build();

        CopyOut.Output runOut = copyOut.run(runContext);
        assertThat(runOut.getRowCount(), is(2L));

        String destination = "d" + IdUtils.create();

        Query create = Query.builder()
                .url(Property.ofValue(TestUtils.url()))
                .username(Property.ofValue(TestUtils.username()))
                .password(Property.ofValue(TestUtils.password()))
                .fetchType(Property.ofValue(FETCH_ONE))
                .sql(Property.ofValue("CREATE TABLE " + destination + " (\n" +
                        "    int INT,\n" +
                        "    bool BOOL" +
                        ");"))
                .build();
        create.run(runContext);

        CopyIn copyIn = CopyIn.builder()
                .url(Property.ofValue(TestUtils.url()))
                .username(Property.ofValue(TestUtils.username()))
                .password(Property.ofValue(TestUtils.password()))
                .from(Property.ofValue(runOut.getUri().toString()))
                .format(Property.ofValue(AbstractCopy.Format.CSV))
                .header(Property.ofValue(true))
                .delimiter(Property.ofValue('\t'))
                .escape(Property.ofValue('"'))
                .table(Property.ofValue(destination))
                .build();

        CopyIn.Output runIn = copyIn.run(runContext);

        assertThat(runIn.getRowCount(), is(2L));
    }

     @Test
    void copyOut_outputModeCopyShouldPreserveDefaultCopyTextRepresentationEscaping() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        var tableName = "copy_out_output_mode_default_" + IdUtils.create();

        String json = """
                {
                  "tab": "\\t",
                  "newline": "\\n",
                  "backslash": "\\\\",
                  "forwardSlash": "/",
                  "quotationMark": "\\\""
                }
                """;

        String sql = createPayloadTable(runContext, tableName, json);

        CopyOut copyOut = CopyOut.builder()
                .url(Property.ofValue(TestUtils.url()))
                .username(Property.ofValue(TestUtils.username()))
                .password(Property.ofValue(TestUtils.password()))
                .format(Property.ofValue(AbstractCopy.Format.TEXT))
                .delimiter(Property.ofValue('\t'))
                .sql(Property.ofValue(sql))
                .build();

        CopyOut.Output output = copyOut.run(runContext);

        String actual;
        try (var in = runContext.storage().getFile(output.getUri())) {
            actual = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        String expected = json
                .replace("\\", "\\\\")
                .replace("\n", "\\n")
                + "\n";

        assertThat(actual, is(expected));
    }

    @Test
    void copyOut_outputModeRawShouldPreserveJsonEscapeCharactersWithoutExtraEscaping() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        var tableName = "copy_out_outputmode_raw_" + IdUtils.create();

        String json = """
                {
                  "tab": "\\t",
                  "newline": "\\n",
                  "backslash": "\\\\",
                  "quotationMark": "\\\""
                }
                """;

        String sql = createPayloadTable(runContext, tableName, json);

        CopyOut copyOut = CopyOut.builder()
                .url(Property.ofValue(TestUtils.url()))
                .username(Property.ofValue(TestUtils.username()))
                .password(Property.ofValue(TestUtils.password()))
                .format(Property.ofValue(AbstractCopy.Format.TEXT))
                .outputMode(Property.ofValue(CopyOut.OutputMode.RAW))
                .delimiter(Property.ofValue('\t'))
                .sql(Property.ofValue(sql))
                .build();

        CopyOut.Output output = copyOut.run(runContext);

        String actual;
        try (var in = runContext.storage().getFile(output.getUri())) {
            actual = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        assertThat(actual, is(json + "\n"));
    }

    @Test
    void copyOut_outputModeRawShouldPreserveUnicodeCharacters() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        var tableName = "copy_out_outputmode_raw_" + IdUtils.create();

        String json = """
                {
                  "tab": "\\t",
                  "escapedNewline": "\\n",
                  "escapedBackslash": "\\\\",
                  "escapedQuotationMark": "\\\"",
                  "nord": "åäö",
                  "emoji": "😺",
                  "glyphs": "❍⍤⨕⺡⛢↢⒬ↅ⢆⊼⒌⁀⩤Ⳃ⻊⏖ℒ⼂↯⸨⟁⦪⫏⺀⊧ⶨ⺝ⶃ✞ⴊ⎡ⱡ⿃⃳⺨⼀ⅆ⼹⭒⍭⌨⠈╇◀▅❚▂⡌⺳⨴⸹ⲍ━⒜⨚"
                }
                """;

        String sql = createPayloadTable(runContext, tableName, json);

        CopyOut copyOut = CopyOut.builder()
                .url(Property.ofValue(TestUtils.url()))
                .username(Property.ofValue(TestUtils.username()))
                .password(Property.ofValue(TestUtils.password()))
                .format(Property.ofValue(AbstractCopy.Format.TEXT))
                .outputMode(Property.ofValue(CopyOut.OutputMode.RAW))
                .delimiter(Property.ofValue('\t'))
                .sql(Property.ofValue(sql))
                .build();

        CopyOut.Output output = copyOut.run(runContext);

        String actual;
        try (var in = runContext.storage().getFile(output.getUri())) {
            actual = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        assertThat(actual, is(json + "\n"));
    }

    @Test
      void copyOut_RawShouldThrowForInvalidFormats() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        var invalidFormats = new AbstractCopy.Format[] {
                AbstractCopy.Format.CSV,
                AbstractCopy.Format.BINARY
        };

        for (AbstractCopy.Format format : invalidFormats) {
            CopyOut copyOut = CopyOut.builder()
                    .format(Property.ofValue(format))
                    .outputMode(Property.ofValue(CopyOut.OutputMode.RAW))
                    .build();
                                        
                    assertThrows(
                        IllegalArgumentException.class,
                        () -> copyOut.run(runContext)
                    );
                }
        }

        private String createPayloadTable(
                RunContext runContext,
                String tableName,
                String json) throws Exception {
            Query create = Query.builder()
            .url(Property.ofValue(TestUtils.url()))
                .username(Property.ofValue(TestUtils.username()))
                .password(Property.ofValue(TestUtils.password()))
                .fetchType(Property.ofValue(NONE))
                .sql(Property.ofValue(
                    "CREATE TABLE " + tableName + " (payload TEXT)"))
                    .build();
                    
                create.run(runContext);                
         return """
                INSERT INTO %s (payload)
                VALUES ($json$%s$json$)
                RETURNING payload
                """.formatted(tableName, json);
    }
}