package io.kestra.plugin.jdbc.druid;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
public class DruidTest {
    @Inject
    RunContextFactory runContextFactory;

    //@BeforeAll
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
    void insertAndQuery() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
                .url("jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/;transparent_reconnection=true")
                .fetchOne(true)
                .timeZoneId("Europe/Paris")
                .sql("select \n" +
                        "  -- NULL as t_null,\n" +
                        "  'string' AS t_string,\n" +
                        "  CAST(2147483647 AS INT) as t_integer,\n" +
                        "  CAST(12345.124 AS FLOAT) as t_float,\n" +
                        "  CAST(12345.124 AS DOUBLE) as t_double,\n" +
                        "  TIME_FORMAT(MILLIS_TO_TIMESTAMP(1639137263000), 'yyyy-MM-dd')  as t_date,\n" +
                        "  TIME_FORMAT(MILLIS_TO_TIMESTAMP(1639137263000), 'yyyy-MM-dd HH:mm:ss') AS t_timestamp\n" +
                        "  \n" +
                        "from restaurant_user_transactions \n" +
                        "limit 1")
                .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("t_null"), is(nullValue()));
        assertThat(runOutput.getRow().get("t_string"), is("string"));
        assertThat(runOutput.getRow().get("t_integer"), is(2147483647));
        assertThat(runOutput.getRow().get("t_float"), is(12345.124));
        assertThat(runOutput.getRow().get("t_double"), is(12345.124D));
        assertThat(runOutput.getRow().get("t_date"), is("2021-12-10"));
        assertThat(runOutput.getRow().get("t_timestamp"), is("2021-12-10 11:54:23"));
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

