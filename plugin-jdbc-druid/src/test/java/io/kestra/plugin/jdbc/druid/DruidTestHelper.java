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
    private static final String BROKER = "http://localhost:11082";

    private DruidTestHelper() {}

    static void initServer() throws IOException, InterruptedException, TimeoutException {
        System.out.println("Waiting for Druid router...");
        waitForRouter();
        System.out.println("Router is ready.");

        System.out.println("Waiting for Druid broker...");
        waitForBroker();
        System.out.println("Broker is ready.");

        System.out.println("Waiting for Druid indexer...");
        waitForIndexer();
        System.out.println("Indexer is ready.");

        cleanupRunningTasks();

        System.out.println("Running inline ingestion...");
        runInlineIngestion();

        System.out.println("Waiting for datasource 'products' to be available...");
        waitForDatasource("products");
        System.out.println("Datasource 'products' is ready.");
    }

    private static void cleanupRunningTasks() throws IOException, InterruptedException {
        var resp = HTTP.send(
            HttpRequest.newBuilder(URI.create(INDEXER + "/druid/indexer/v1/runningTasks")).GET().build(),
            HttpResponse.BodyHandlers.ofString()
        );

        if (resp.statusCode() == 200) {
            var tasks = MAPPER.readTree(resp.body());
            for (var t : tasks) {
                var id = t.get("id").asText();
                System.out.println("Shutting down running task: " + id);
                HTTP.send(
                    HttpRequest.newBuilder(URI.create(INDEXER + "/druid/indexer/v1/task/" + id + "/shutdown"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                    HttpResponse.BodyHandlers.ofString()
                );
            }
        }
    }

    private static void waitForRouter() throws TimeoutException {
        Await.until(() -> isServiceUp(ROUTER + "/status"), Duration.ofSeconds(2), Duration.ofMinutes(5));
    }

    private static void waitForBroker() throws TimeoutException {
        Await.until(() -> isServiceUp(BROKER + "/status"), Duration.ofSeconds(2), Duration.ofMinutes(5));
    }

    private static void waitForIndexer() throws TimeoutException {
        Await.until(() -> {
            if (isServiceUp(INDEXER + "/status")) return true;
            // fallback check in case /status is not yet active
            return isServiceUp(INDEXER + "/druid/indexer/v1/runningTasks");
        }, Duration.ofSeconds(2), Duration.ofMinutes(6));
    }

    private static boolean isServiceUp(String url) {
        try {
            var response = HTTP.send(
                HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .GET().build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return response.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
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

        var stmtUri = URI.create(ROUTER + "/druid/v2/sql/statements");
        var stmtRequest = HttpRequest.newBuilder(stmtUri)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build();

        var stmtResponse = HTTP.send(stmtRequest, HttpResponse.BodyHandlers.ofString());
        var stmtJson = MAPPER.readTree(stmtResponse.body());
        var queryIdNode = stmtJson.get("queryId");

        if (queryIdNode == null || queryIdNode.isNull()) {
            System.out.println("Warning: Druid ingestion did not return a queryId. Response: " + stmtResponse.body());
            return;
        }

        var queryId = queryIdNode.asText();
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
        }, Duration.ofSeconds(3), Duration.ofMinutes(5));
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
        }, Duration.ofSeconds(3), Duration.ofMinutes(5));
    }
}
