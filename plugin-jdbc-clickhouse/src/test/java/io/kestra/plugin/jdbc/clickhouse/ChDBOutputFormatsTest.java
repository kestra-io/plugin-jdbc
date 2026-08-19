package io.kestra.plugin.jdbc.clickhouse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChDBOutputFormatsTest {

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void defaultAndBlankFormatsStoreAsIon(String format) {
        assertThat(ChDBOutputFormats.shouldStoreAsIon(format), is(true));
        assertThat(ChDBOutputFormats.effectiveClickHouseFormat(format), is("JSONEachRow"));
        assertThat(ChDBOutputFormats.fileExtension(format), is(".ion"));
        assertThat(ChDBOutputFormats.outputFileName(format), is("result.ion"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "PrettyCompact",
        "Pretty",
        "PrettySpace",
        "PrettyMonoBlock",
        "prettycompact",
        "PRETTY"
    })
    void prettyFormatsStoreAsIon(String format) {
        assertThat(ChDBOutputFormats.shouldStoreAsIon(format), is(true));
        assertThat(ChDBOutputFormats.effectiveClickHouseFormat(format), is("JSONEachRow"));
        assertThat(ChDBOutputFormats.fileExtension(format), is(".ion"));
    }

    @ParameterizedTest
    @CsvSource({
        "CSVWithNames, .csv",
        "CSV, .csv",
        "CSVWithNamesAndTypes, .csv",
        "TabSeparated, .tsv",
        "TabSeparatedWithNames, .tsv",
        "TSV, .tsv",
        "JSONEachRow, .json",
        "JSON, .json",
        "JSONCompact, .json",
        "Parquet, .parquet",
        "ORC, .orc",
        "Avro, .avro",
        "Arrow, .arrow",
        "ArrowStream, .arrow",
        "Native, .bin",
        "XML, .xml",
        "Markdown, .md"
    })
    void fileFormatsMapToDedicatedExtensions(String format, String extension) {
        assertThat(ChDBOutputFormats.shouldStoreAsIon(format), is(false));
        assertThat(ChDBOutputFormats.effectiveClickHouseFormat(format), is(format));
        assertThat(ChDBOutputFormats.fileExtension(format), is(extension));
        assertThat(ChDBOutputFormats.outputFileName(format), is("result" + extension));
    }

    @Test
    void trimsUserFormatBeforeUse() {
        assertThat(ChDBOutputFormats.effectiveClickHouseFormat("  CSVWithNames  "), is("CSVWithNames"));
        assertThat(ChDBOutputFormats.fileExtension("  CSVWithNames  "), is(".csv"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "CSVWithNames",
        "JSONEachRow",
        "PrettyCompact",
        "Parquet"
    })
    void acceptsAlphanumericFormats(String format) {
        assertThat(ChDBOutputFormats.requireSafeFormat(format), is(format));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "CSV; rm -rf /",
        "CSV&&id",
        "CSV`id`",
        "CSV$(id)",
        "CSV WithNames",
        "CSV_WithNames",
        ""
    })
    void rejectsUnsafeFormats(String format) {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> ChDBOutputFormats.requireSafeFormat(format)
        );
        assertThat(exception.getMessage(), containsString("Invalid outputFormat"));
    }

    @Test
    void rejectsNullFormat() {
        assertThrows(IllegalArgumentException.class, () -> ChDBOutputFormats.requireSafeFormat(null));
    }
}
