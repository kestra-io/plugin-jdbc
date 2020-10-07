package org.kestra.task.jdbc;

import org.junit.jupiter.api.Test;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.flows.State;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * This test will load all flow located in `src/test/resources/flows/`
 * and will run an in-memory runner to be able to test a full flow. There is also a
 * configuration file in `src/test/resources/application.yml` that is only for the full runner
 * test to configure in-memory runner.
 */

class JdbcQueryRunnerTest extends AbstractRunnerTest {

    @SuppressWarnings("unchecked")
    @Test
    void fetchOneRow() throws TimeoutException {
        Execution execution = runnerUtils.runOne("org.kestra.jdbc", "fetch_one");

        assertThat(execution.getTaskRunList(), hasSize(1));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        Map<String, String> outputRow = (Map<String, String>) execution.getTaskRunList().get(0).getOutputs().get("row");
        checkFirstRow(outputRow);
    }

    private void checkFirstRow(Map<String, String> firstRow) {
        assertThat(firstRow.get("ID"), is("4814976"));
        assertThat(firstRow.get("ENUM"), is("Female"));
        assertThat(firstRow.get("DATE"), is("2017/11/16"));
        assertThat(firstRow.get("TIMESTAMPMICROS"), is("2013-02-05T11:07:59Z"));
        assertThat(firstRow.get("TIMEMILLIS"), is("5:14"));
        assertThat(firstRow.get("EMAIL"), is("alidgertwood0@nbcnews.com"));
        assertThat(firstRow.get("BOOLEAN"), is("true"));
        assertThat(firstRow.get("NAME"), is("Viva"));
        assertThat(firstRow.get("LONG"), is("-867174862080523000"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void fetchRows() throws TimeoutException, IOException, ClassNotFoundException, URISyntaxException {
        Execution execution = runnerUtils.runOne("org.kestra.jdbc", "fetch");

        assertThat(execution.getTaskRunList(), hasSize(1));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        String outputRowsUri = (String) execution.getTaskRunList().get(0).getOutputs().get("uri");

        assertThat(outputRowsUri, notNullValue());

        /*ObjectInputStream in = new ObjectInputStream(this.storageInterface.get(new URI(outputRowsUri)));
        // FIXME : This is not a list that we stored but individual objects
        List<Map<String, String>> rows = (List<Map<String, String>>) in.readObject();
        in.close();
        checkFirstRow(rows.get(0));*/
    }
}
