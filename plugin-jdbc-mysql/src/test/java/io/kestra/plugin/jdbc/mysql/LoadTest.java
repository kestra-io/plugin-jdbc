package io.kestra.plugin.jdbc.mysql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.sql.SQLException;
import java.util.Objects;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static io.kestra.core.models.tasks.common.FetchType.NONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class LoadTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @Test
    void load() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URL resource = LoadTest.class.getClassLoader().getResource("load.csv");

        URI put = storageInterface.put(
            null,
            null,
            new URI("/file/storage/get.yml"),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        Query createTable = Query.builder()
            .url("jdbc:mysql://127.0.0.1:64790/kestra")
            .username("root")
            .password("mysql_passwd")
            .sql("CREATE TABLE IF NOT EXISTS discounts (\n" +
                "    id INT NOT NULL AUTO_INCREMENT,\n" +
                "    title VARCHAR(255) NOT NULL,\n" +
                "    expired_date DATE NOT NULL,\n" +
                "    amount DECIMAL(10 , 2 ) NULL,\n" +
                "    PRIMARY KEY (id)\n" +
                ");")
            .fetchType(NONE)
            .build();

        createTable.run(runContext);

        Query load = Query.builder()
            .url("jdbc:mysql://127.0.0.1:64790/kestra?allowLoadLocalInfile=true")
            .username("root")
            .password("mysql_passwd")
            .inputFile(put.toString())
            .fetchType(NONE)
            .sql("LOAD DATA LOCAL INFILE '{{ inputFile }}' \n" +
                "INTO TABLE discounts \n" +
                "FIELDS TERMINATED BY ',' \n" +
                "ENCLOSED BY '\"'\n" +
                "LINES TERMINATED BY '\\n'\n" +
                "IGNORE 1 ROWS;")
            .build();


        AbstractJdbcQuery.Output loadRun = load.run(runContext);

        Query out = Query.builder()
            .url("jdbc:mysql://127.0.0.1:64790/kestra")
            .username("root")
            .password("mysql_passwd")
            .fetchType(FETCH_ONE)
            .sql("SELECT COUNT(*) as count FROM discounts \n")
            .build();

        AbstractJdbcQuery.Output outRun = out.run(runContext);

        assertThat(outRun.getRow(), notNullValue());
        assertThat(outRun.getRow().get("count"), is(3L));
    }


    @Test
    void onlyTmp() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query createTable = Query.builder()
            .url("jdbc:mysql://127.0.0.1:64790/kestra")
            .username("root")
            .password("mysql_passwd")
            .sql("CREATE TABLE IF NOT EXISTS passwd (\n" +
                "    password TEXT\n" +
                ");")
            .build();

        createTable.run(runContext);

        Query load = Query.builder()
            .url("jdbc:mysql://127.0.0.1:64790/kestra?allowLoadLocalInfile=true")
            .username("root")
            .password("mysql_passwd")
            .sql("LOAD DATA LOCAL INFILE '/etc/passwd' \n" +
                "INTO TABLE passwd \n" +
                "FIELDS TERMINATED BY ',' \n" +
                "ENCLOSED BY '\"'\n" +
                "LINES TERMINATED BY '\\n'\n" +
                "IGNORE 1 ROWS;")
            .build();

        SQLException e = assertThrows(SQLException.class, () -> load.run(runContext));

        assertThat(e.getMessage(), containsString("/etc/passwd"));
    }
}
