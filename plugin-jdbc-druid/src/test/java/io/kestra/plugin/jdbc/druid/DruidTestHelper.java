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
import java.util.concurrent.TimeoutException;

final class DruidTestHelper {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();

    private DruidTestHelper() {
    }

    static void initServer() throws IOException, InterruptedException, TimeoutException {
        String payload = """
                {
                    "context": {"waitUntilSegmentsLoad": true, "finalizeAggregations": false, "groupByEnableMultiValueUnnesting": false, "executionMode":"async", "maxNumTasks": 2},
                    "header": true,
                    "query": "REPLACE INTO \\"products\\" OVERWRITE ALL WITH \\"ext\\" AS (  SELECT * FROM TABLE(EXTERN('{\\"type\\":\\"http\\",\\"uris\\":[\\"https://drive.google.com/uc?id=1OT84-j5J5z2tHoUvikJtoJFInWmlyYzY&export=download\\"]}',\\n      '{\\"type\\":\\"csv\\",\\"findColumnsFromHeader\\":true}'\\n    )\\n  ) EXTEND (\\"index\\" BIGINT, \\"name\\" VARCHAR, \\"ean\\" BIGINT)) SELECT  TIMESTAMP '2000-01-01 00:00:00' AS \\"__time\\", \\"index\\",  \\"name\\",  \\"ean\\"FROM \\"ext\\" PARTITIONED BY ALL",
                    "resultFormat": "array",
                    "sqlTypesHeader": true,
                    "typesHeader": true
                }""";
        var httpClient = HttpClient.newHttpClient();
        URI uri = URI.create("http://localhost:8888/druid/v2/sql/statements");
        var httpRequest = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();
        var httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        var json = OBJECT_MAPPER.readTree(httpResponse.body());
        String queryId = json.get("queryId").asText();

        // we need to wait until Druid has processed the request
        Await.until(() -> {
            try {
                URI queryUri =  URI.create("http://localhost:8888/druid/v2/sql/statements/"+ queryId);
                var queryHttpRequest = HttpRequest.newBuilder(queryUri)
                    .header("Accept", "application/json")
                    .GET()
                    .build();
                var queryHttpResponse = httpClient.send(queryHttpRequest, HttpResponse.BodyHandlers.ofString());
                var queryJson = OBJECT_MAPPER.readTree(queryHttpResponse.body());
                String state = queryJson.get("state").asText();
                return "SUCCESS".equals(state);
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }, Duration.ofSeconds(1), Duration.ofMinutes(1));
    }
}
