package io.kestra.plugin.jdbc.druid;

import com.fasterxml.jackson.databind.JsonNode;
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
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final String ROUTER = "http://localhost:8888";
    private static final String COORDINATOR = "http://localhost:11081";
    private static final String INDEXER = "http://localhost:8888/druid/indexer/v1";

    private DruidTestHelper() {}

    static void initServer() throws IOException, InterruptedException, TimeoutException {
        System.out.println("====== INITIALIZING DRUID ======");
        waitForRouter();
        waitForCoordinator();
        cleanupRunningTasks();
        runInlineIngestion();
        waitForDatasource("products");
        System.out.println("====== DRUID READY ======");
    }

    // ------------------------------------------------------------------------
    // Health Checks
    // ------------------------------------------------------------------------
    private static void waitForRouter() throws TimeoutException {
        System.out.println("Waiting for Druid router...");
        Await.until(() -> isServiceUp(ROUTER + "/status"), Duration.ofSeconds(2), Duration.ofMinutes(5));
        System.out.println("✓ Router ready");
    }

    private static void waitForCoordinator() throws TimeoutException {
        System.out.println("Waiting for Druid coordinator...");
        Await.until(() -> isServiceUp(COORDINATOR + "/status"), Duration.ofSeconds(2), Duration.ofMinutes(5));
        System.out.println("✓ Coordinator ready");
    }

    private static boolean isServiceUp(String url) {
        try {
            HttpResponse<String> resp = HTTP.send(
                HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .GET().build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return resp.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    // ------------------------------------------------------------------------
    // Cleanup old tasks
    // ------------------------------------------------------------------------
    private static void cleanupRunningTasks() throws IOException, InterruptedException {
        HttpResponse<String> resp = HTTP.send(
            HttpRequest.newBuilder(URI.create(INDEXER + "/runningTasks"))
                .GET().build(),
            HttpResponse.BodyHandlers.ofString()
        );

        if (resp.statusCode() == 200) {
            JsonNode tasks = MAPPER.readTree(resp.body());
            for (JsonNode t : tasks) {
                String id = t.get("id").asText();
                System.out.println("Shutting down leftover task: " + id);
                HTTP.send(
                    HttpRequest.newBuilder(URI.create(INDEXER + "/task/" + id + "/shutdown"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                    HttpResponse.BodyHandlers.ofString()
                );
            }
        }
    }

    // ------------------------------------------------------------------------
    // Ingestion
    // ------------------------------------------------------------------------
    private static void runInlineIngestion() throws IOException, InterruptedException, TimeoutException {
        System.out.println("Starting inline ingestion for 'products'...");

        String query = """
            REPLACE INTO products OVERWRITE ALL
            WITH ext AS (
              SELECT * FROM TABLE(EXTERN(
                '{"type":"inline","data":"Index,Name\\n1,John\\n2,Alice\\n3,Bob\\n4,Carol\\n5,David"}',
                '{"type":"csv","findColumnsFromHeader":true}'
              )) EXTEND ("Index" BIGINT, "Name" VARCHAR)
            )
            SELECT TIME_PARSE('2000-01-01 00:00:00') AS "__time", * FROM ext PARTITIONED BY ALL
            """;

        String payload = MAPPER.writeValueAsString(Map.of(
            "context", Map.of("executionMode", "ASYNC", "maxNumTasks", 2),
            "query", query,
            "resultFormat", "array"
        ));

        HttpResponse<String> resp = HTTP.send(
            HttpRequest.newBuilder(URI.create(ROUTER + "/druid/v2/sql/statements"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );

        if (resp.statusCode() != 200) {
            throw new IOException("Ingestion request failed: " + resp.statusCode() + " - " + resp.body());
        }

        JsonNode json = MAPPER.readTree(resp.body());
        String queryId = json.path("queryId").asText(null);
        if (queryId == null) {
            System.err.println("⚠ No queryId returned. Full response: " + resp.body());
        } else {
            System.out.println("✓ Ingestion submitted, queryId=" + queryId);
        }

        // Wait for any successful ingestion task
        Await.until(DruidTestHelper::hasSuccessfulIngestionTask, Duration.ofSeconds(3), Duration.ofMinutes(5));
        System.out.println("✓ Ingestion completed successfully.");
    }

    private static boolean hasSuccessfulIngestionTask() {
        try {
            HttpResponse<String> resp = HTTP.send(
                HttpRequest.newBuilder(URI.create(INDEXER + "/completeTasks?max=5"))
                    .GET().build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return resp.statusCode() == 200 && resp.body().contains("\"statusCode\":\"SUCCESS\"");
        } catch (Exception e) {
            return false;
        }
    }

    // ------------------------------------------------------------------------
    // Datasource Availability
    // ------------------------------------------------------------------------
    private static void waitForDatasource(String datasource) throws TimeoutException {
        System.out.println("Waiting for datasource '" + datasource + "'...");
        Await.until(() -> {
            try {
                HttpResponse<String> resp = HTTP.send(
                    HttpRequest.newBuilder(URI.create(COORDINATOR + "/druid/coordinator/v1/datasources"))
                        .GET().build(),
                    HttpResponse.BodyHandlers.ofString()
                );
                return resp.statusCode() == 200 && resp.body().contains(datasource);
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofSeconds(3), Duration.ofMinutes(5));
        System.out.println("✓ Datasource '" + datasource + "' ready.");
    }
}
