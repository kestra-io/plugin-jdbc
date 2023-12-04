package io.kestra.plugin.jdbc.druid;

import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class DruidTriggerTest extends AbstractJdbcTriggerTest {

    @BeforeAll
    public static void startServer() throws Exception {
        String payload = "{" +
                "            \"context\": " +
                "            {\"waitUntilSegmentsLoad\": true, " +
                "                    \"finalizeAggregations\": false, \"groupByEnableMultiValueUnnesting\": false, " +
                "                    \"executionMode\":\"async\", \"maxNumTasks\": 2}, " +
                "            \"header\": true, " +
                "            \"query\": " +
                "        \"REPLACE INTO \\\"products\\\" OVERWRITE ALL WITH \\\"ext\\\" AS (  SELECT * FROM TABLE(EXTERN('{\\\"type\\\":\\\"http\\\",\\\"uris\\\":[\\\"https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/products/products-1000.csv\\\"]}',\\n      '{\\\"type\\\":\\\"csv\\\",\\\"findColumnsFromHeader\\\":true}'\\n    )\\n  ) EXTEND (\\\"index\\\" BIGINT, \\\"name\\\" VARCHAR, \\\"ean\\\" BIGINT)) SELECT  TIMESTAMP '2000-01-01 00:00:00' AS \\\"__time\\\", \\\"index\\\",  \\\"name\\\",  \\\"ean\\\"FROM \\\"ext\\\"PARTITIONED BY ALL\"," +
                "        \"resultFormat\": \"array\", " +
                "        \"sqlTypesHeader\": true, " +
                "        \"typesHeader\": true}";
        URL obj = new URL("http://localhost:8888/druid/v2/sql/statements");
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type","application/json");
        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(payload);
        wr.flush();
        wr.close();

        String response = getResponseFromConnection(con);
        JSONObject jsonObjectResponse = new JSONObject(response);
        String queryId = jsonObjectResponse.getString("queryId");
        String state = jsonObjectResponse.getString("state");
        while (!state.equals("SUCCESS")) {
            TimeUnit.SECONDS.sleep(30);
            HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:8888/druid/v2/sql/statements/"+ queryId).openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type","application/json");
            connection.setDoOutput(true);
            String getCallResonse = getResponseFromConnection(connection);
            System.out.println(getCallResonse);
            jsonObjectResponse = new JSONObject(getCallResonse);
            queryId = jsonObjectResponse.getString("queryId");
            state = jsonObjectResponse.getString("state");
        }
    }

    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","druid-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Override
    protected String getUrl() {
        return "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/;transparent_reconnection=true";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        // nothing here, already done in @BeforeAll.
    }

    public static String getResponseFromConnection(HttpURLConnection connection) throws Exception {
        int responseCode = connection.getResponseCode();
        System.out.println("Response Code : " + responseCode);

        BufferedReader iny = new BufferedReader(
                new InputStreamReader(connection.getInputStream()));
        String output;
        StringBuffer response = new StringBuffer();

        while ((output = iny.readLine()) != null) {
            response.append(output);
        }
        iny.close();
        return response.toString();
    }
}