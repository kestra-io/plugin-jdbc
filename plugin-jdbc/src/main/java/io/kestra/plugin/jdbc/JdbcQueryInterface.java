package io.kestra.plugin.jdbc;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.common.FetchType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;


public interface JdbcQueryInterface extends JdbcStatementInterface {
    @Schema(
        title = "The SQL query to run."
    )
    @PluginProperty(dynamic = true)
    String getSql();

    @Schema(
        title = "DEPRECATED, should use `fetchType: FETCH` instead)" + "\n" +
            "Whether to fetch the data from the query result to the task output.(DEPRECATED, should use `fetchType` instead)" +
            " This parameter is evaluated after `fetchOne` and `store`."
    )
    @PluginProperty(dynamic = false)
    boolean isFetch();

    @Schema(
        title = "DEPRECATED, should use `fetchType: FETCH_STORE` instead)" + "\n" +
            "Whether to fetch data row(s) from the query result to a file in internal storage." +
            " File will be saved as Amazon Ion (text format)." +
            " \n" +
            " See <a href=\"http://amzn.github.io/ion-docs/\">Amazon Ion documentation</a>" +
            " This parameter is evaluated after `fetchOne` but before `fetch`."
    )
    @PluginProperty(dynamic = false)
    boolean isStore();

    @Schema(
        title = "DEPRECATED, should use `fetchType: FETCH_ONE` instead)" + "\n" +
            "Whether to fetch only one data row from the query result to the task output.(DEPRECATED, should use `fetchType` instead)" +
            " This parameter is evaluated before `store` and `fetch`."
    )
    @PluginProperty(dynamic = false)
    boolean isFetchOne();

    @Schema(
        title = "Number of rows that should be fetched.",
        description = "Gives the JDBC driver a hint as to the number of rows that should be fetched from the database " +
            "when more rows are needed for this ResultSet object. If the fetch size specified is zero, the JDBC driver " +
            "ignores the value and is free to make its own best guess as to what the fetch size should be. Ignored if " +
            "`autoCommit` is false."
    )
    @PluginProperty(dynamic = false)
    Integer getFetchSize();

    @Schema(
            title = "The way you want to store data.",
            description = "FETCH_ONE - output the first row.\n"
                    + "FETCH - output all rows as output variable.\n"
                    + "STORE - store all rows to a file.\n"
                    + "NONE - do nothing."
    )
    @PluginProperty
    @NotNull
    FetchType getFetchType();
}
