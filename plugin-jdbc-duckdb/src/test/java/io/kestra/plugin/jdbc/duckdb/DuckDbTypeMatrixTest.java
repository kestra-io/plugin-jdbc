package io.kestra.plugin.jdbc.duckdb;

import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class DuckDbTypeMatrixTest {
    private TimeZone originalTimeZone;

    @BeforeEach
    void useCanonicalTimeZone() {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @AfterEach
    void restoreTimeZone() {
        TimeZone.setDefault(originalTimeZone);
    }

    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @ParameterizedTest
    @EnumSource(value = FetchType.class, names = {"FETCH", "FETCH_ONE", "STORE"})
    void preservesLegacyTypeValuesAndMaterializesVariants(FetchType mode) throws Exception {
        var baseline = JacksonMapper.ofJson().readTree(resource("type-matrix-1.5.6.json")).get(mode.name());
        var expected = JacksonMapper.ofJson().readTree(resource("type-matrix-2.0.0.json")).get(mode.name());
        Map<String, String> types = new LinkedHashMap<>();
        for (String line : resource("types.txt").lines().toList()) {
            String[] parts = line.split("\\|", 2);
            types.put(parts[0], parts[1]);
        }
        for (var type : types.entrySet()) {
            String name = type.getKey();
            String sql = "SELECT " + type.getValue() + " AS value UNION ALL SELECT NULL AS value";
            var task = Query.builder()
                .communityExtensions(Property.ofValue(List.of()))
                .timeZoneId(Property.ofValue("UTC"))
                .sql(Property.ofValue(sql))
                .fetchType(Property.ofValue(mode))
                .build();
            if (expected.get(name).has("error")) {
                // Four types were already unsupported on 1.5.6. Keep their
                // limitations explicit rather than treating a new failure as a pass.
                assertEquals(baseline.get(name).get("error"), expected.get(name).get("error"), name);
                var failure = assertThrows(IllegalArgumentException.class,
                    () -> task.run(runContextFactory.of(Map.of())), name);
                assertThat(name, failure.getMessage(), containsString("not supported"));
                assertThat(name, failure.getMessage(), containsString(baseline.get(name).get("jdbcType").asText()));
                continue;
            }
            if (!"variant".equals(name) && !"timestamptz_ns".equals(name)) {
                assertEquals(baseline.get(name).get("rows"), expected.get(name).get("rows"), name);
            }
            var output = task.run(runContextFactory.of(Map.of()));
            Object rows;
            if (mode == FetchType.STORE) {
                try (var input = storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())) {
                    rows = FileSerde.readAll(input).collectList().block();
                }
            } else {
                rows = mode == FetchType.FETCH_ONE ? List.of(output.getRow()) : output.getRows();
            }
            // A JSON wire-format comparison retains numeric precision and nulls,
            // while avoiding irrelevant Float-vs-Double JsonNode class differences.
            var mapper = JacksonMapper.ofJson().copy()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS);
            JsonNode actual = mapper.readTree(mapper.writeValueAsString(rows));
            assertEquals(expected.get(name).get("rows"), actual, name + " / " + mode);
        }
    }

    private String resource(String name) throws Exception {
        try (var input = Objects.requireNonNull(getClass().getResourceAsStream("/compatibility/" + name))) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
