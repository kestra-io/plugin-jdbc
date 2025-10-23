package io.kestra.plugin.jdbc.druid;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Await;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;

final class DruidTestHelper {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final String ROUTER = "http://localhost:8888";
    private static final String INDEXER = "http://localhost:11081";

    private DruidTestHelper() {}

    static void initServer() throws IOException, InterruptedException, TimeoutException {
        waitForRouter();
        waitForIndexer();
        cleanupRunningTasks();
        runInlineIngestion();
        waitForIndexer();
        waitForDatasource("products");
    }

    private static void cleanupRunningTasks() throws IOException, InterruptedException {
        var resp = HTTP.send(HttpRequest.newBuilder(URI.create(INDEXER + "/druid/indexer/v1/runningTasks")).GET().build(), HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() == 200) {
            var tasks = MAPPER.readTree(resp.body());
            for (var t : tasks) {
                var id = t.get("id").asText();
                HTTP.send(
                    HttpRequest.newBuilder(URI.create(INDEXER + "/druid/indexer/v1/task/" + id + "/shutdown"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                    HttpResponse.BodyHandlers.ofString()
                );
            }
        }
    }

    private static void waitForIndexer() throws TimeoutException {
        Await.until(() -> {
            try {
                var r = HTTP.send(
                    HttpRequest.newBuilder(URI.create(INDEXER + "/status")).GET().build(),
                    HttpResponse.BodyHandlers.ofString()
                );
                return r.statusCode() == 200;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(2), Duration.ofMinutes(3));
    }

    private static void waitForRouter() throws TimeoutException {
        Await.until(() -> {
            try {
                var r = HTTP.send(
                    HttpRequest.newBuilder(URI.create(ROUTER + "/status")).GET().build(),
                    HttpResponse.BodyHandlers.ofString()
                );
                return r.statusCode() == 200;
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(2), Duration.ofMinutes(3));
    }

    private static void runInlineIngestion() throws IOException, InterruptedException, TimeoutException {
        var query = """
            REPLACE INTO "products" OVERWRITE ALL
            WITH "ext" AS (
              SELECT * FROM TABLE(EXTERN(
                '{"type":"inline","data":"Index,Name\\n1,John\\n2,Alice\\n3,Bob\\n4,Carol\\n5,David"}',
                '{"type":"csv","findColumnsFromHeader":true}'
              )) EXTEND ("Index" BIGINT, "Name" VARCHAR)
            )
            SELECT TIME_PARSE('2000-01-01 00:00:00') AS "__time", * FROM "ext" PARTITIONED BY ALL
            """;

        var payload = MAPPER.writeValueAsString(Map.of(
            "context", Map.of("executionMode", "ASYNC", "maxNumTasks", 2),
            "query", query,
            "resultFormat", "array"
        ));

        var stmtUri = URI.create("http://localhost:8888/druid/v2/sql/statements");
        var stmtRequest = HttpRequest.newBuilder(stmtUri)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build();

        var stmtResponse = HTTP.send(stmtRequest, HttpResponse.BodyHandlers.ofString());
        var stmtJson = MAPPER.readTree(stmtResponse.body());
        var queryId = stmtJson.get("queryId").asText();

        Await.until(() -> {
            try {
                var tasks = HTTP.send(
                    HttpRequest.newBuilder(URI.create(INDEXER + "/druid/indexer/v1/runningTasks"))
                        .GET().build(),
                    HttpResponse.BodyHandlers.ofString()
                );
                return tasks.statusCode() == 200 && tasks.body().contains(queryId);
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(3), Duration.ofMinutes(3));
    }

    private static void waitForDatasource(String datasource) throws TimeoutException {
        Await.until(() -> {
            try {
                var resp = HTTP.send(
                    HttpRequest.newBuilder(URI.create(INDEXER + "/druid/coordinator/v1/datasources"))
                        .GET().build(),
                    HttpResponse.BodyHandlers.ofString()
                );
                return resp.statusCode() == 200 && resp.body().contains(datasource);
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(3), Duration.ofMinutes(3));
    }
}
