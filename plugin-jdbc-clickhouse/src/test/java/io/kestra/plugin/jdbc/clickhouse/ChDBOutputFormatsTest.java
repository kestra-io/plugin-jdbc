package io.kestra.plugin.jdbc.clickhouse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
}
