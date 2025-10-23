package io.kestra.plugin.jdbc.druid;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Await;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;

final class DruidTestHelper {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();

    private DruidTestHelper() {
    }

    static void initServer() throws IOException, InterruptedException, TimeoutException {
        var httpClient = HttpClient.newHttpClient();

        Await.until(() -> {
            try {
                var resp = httpClient.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:8888/status")).GET().build(),
                    HttpResponse.BodyHandlers.ofString()
                );
                return resp.statusCode() == 200;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(2), Duration.ofMinutes(1));

        var query = """
            REPLACE INTO "products" OVERWRITE ALL
            WITH "ext" AS (
              SELECT * FROM TABLE(EXTERN(
                '{"type":"inline","data":"Index,Name\\n1,John\\n2,Alice\\n3,Bob"}',
                '{"type":"csv","findColumnsFromHeader":true}'
              )) EXTEND ("Index" BIGINT, "Name" VARCHAR)
            )
            SELECT TIMESTAMP '2000-01-01 00:00:00' AS "__time", * FROM "ext" PARTITIONED BY ALL
            """;

        var payload = OBJECT_MAPPER.writeValueAsString(Map.of(
            "context", Map.of(
                "waitUntilSegmentsLoad", true,
                "executionMode", "ASYNC"
            ),
            "query", query,
            "resultFormat", "array"
        ));

        var stmtUri = URI.create("http://localhost:8888/druid/v2/sql/statements");
        var stmtRequest = HttpRequest.newBuilder(stmtUri)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build();

        var stmtResponse = httpClient.send(stmtRequest, HttpResponse.BodyHandlers.ofString());
        var stmtJson = OBJECT_MAPPER.readTree(stmtResponse.body());
        var queryId = stmtJson.get("queryId").asText();

        // we wait for ingestion task to complete
        Await.until(() -> {
            try {
                var tasks = httpClient.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:11081/druid/indexer/v1/completeTasks"))
                        .GET().build(),
                    HttpResponse.BodyHandlers.ofString()
                );
                return tasks.statusCode() == 200 && tasks.body().contains(queryId);
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(5), Duration.ofMinutes(1));
    }
}
