package io.kestra.plugin.jdbc.mariadb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.jdbc.AbstractJdbcBaseQuery;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.sql.SQLException;
import java.util.Objects;

import static io.kestra.core.models.tasks.common.FetchType.*;
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
            TenantService.MAIN_TENANT,
            null,
            new URI("/file/storage/get.yml"),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        Query createTable = Query.builder()
            .url(Property.of("jdbc:mariadb://127.0.0.1:64791/kestra"))
            .username(Property.of("root"))
            .password(Property.of("mariadb_passwd"))
            .sql(Property.of("CREATE TABLE IF NOT EXISTS discounts (\n" +
                "    id INT NOT NULL AUTO_INCREMENT,\n" +
                "    title VARCHAR(255) NOT NULL,\n" +
                "    expired_date DATE NOT NULL,\n" +
                "    amount DECIMAL(10 , 2 ) NULL,\n" +
                "    PRIMARY KEY (id)\n" +
                ");"))
            .fetchType(Property.of(NONE))
            .build();

        createTable.run(runContext);

        Query load = Query.builder()
            .url(Property.of("jdbc:mariadb://127.0.0.1:64791/kestra?allowLoadLocalInfile=true"))
            .username(Property.of("root"))
            .password(Property.of("mariadb_passwd"))
            .inputFile(put.toString())
            .fetchType(Property.of(NONE))
            .sql(new Property<>("LOAD DATA LOCAL INFILE '{{ inputFile }}' \n" +
                "INTO TABLE discounts \n" +
                "FIELDS TERMINATED BY ',' \n" +
                "ENCLOSED BY '\"'\n" +
                "LINES TERMINATED BY '\\n'\n" +
                "IGNORE 1 ROWS;"))
            .build();


        AbstractJdbcQuery.Output loadRun = load.run(runContext);

        Query out = Query.builder()
            .url(Property.of("jdbc:mariadb://127.0.0.1:64791/kestra"))
            .username(Property.of("root"))
            .password(Property.of("mariadb_passwd"))
            .fetchType(Property.of(FETCH_ONE))
            .sql(Property.of("SELECT COUNT(*) as count FROM discounts \n"))
            .build();

        AbstractJdbcQuery.Output outRun = out.run(runContext);

        assertThat(outRun.getRow(), notNullValue());
        assertThat(outRun.getRow().get("count"), is(3L));
    }

    @Test
    void onlyTmp() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query createTable = Query.builder()
            .url(Property.of("jdbc:mariadb://127.0.0.1:64791/kestra"))
            .username(Property.of("root"))
            .password(Property.of("mariadb_passwd"))
            .sql(Property.of("CREATE TABLE IF NOT EXISTS passwd (\n" +
                "    password TEXT\n" +
                ");"))
            .build();

        createTable.run(runContext);

        Query load = Query.builder()
            .url(Property.of("jdbc:mariadb://127.0.0.1:64791/kestra?allowLoadLocalInfile=true"))
            .username(Property.of("root"))
            .password(Property.of("mariadb_passwd"))
            .sql(Property.of("LOAD DATA LOCAL INFILE '/etc/passwd' \n" +
                "INTO TABLE passwd \n" +
                "FIELDS TERMINATED BY ',' \n" +
                "ENCLOSED BY '\"'\n" +
                "LINES TERMINATED BY '\\n'\n" +
                "IGNORE 1 ROWS;"))
            .build();

        load.run(runContext);

        Query result = Query.builder()
            .url(Property.of("jdbc:mariadb://127.0.0.1:64791/kestra?allowLoadLocalInfile=true"))
            .username(Property.of("root"))
            .password(Property.of("mariadb_passwd"))
            .sql(Property.of("SELECT * FROM passwd"))
            .fetchType(Property.of(FETCH))
            .build();


        // FIXME: This test show a potential security leak blocked on mysql
        //  preventing to read a file outside of working directory and not on mariadb
        AbstractJdbcBaseQuery.Output run = result.run(runContext);
        assertThat(run.getRows().size(), greaterThan(0));
    }
}
