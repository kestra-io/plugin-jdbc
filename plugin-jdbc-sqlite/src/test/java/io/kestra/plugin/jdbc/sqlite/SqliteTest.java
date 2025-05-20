package io.kestra.plugin.jdbc.sqlite;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;
import java.util.Properties;

import static io.kestra.core.models.tasks.common.FetchType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class SqliteTest extends AbstractRdbmsTest {
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select * from lite_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("id"), is(1));

        // May be boolean
        assertThat(runOutput.getRow().get("boolean_column"), is(1));
        assertThat(runOutput.getRow().get("text_column"), is("Sample Text"));
        assertThat(runOutput.getRow().get("d"), nullValue());

        assertThat(runOutput.getRow().get("float_column"), is(3.14));
        assertThat(runOutput.getRow().get("double_column"), is(3.14159265359d));
        assertThat(runOutput.getRow().get("int_column"), is(42));

        assertThat(runOutput.getRow().get("date_column"), is(LocalDate.parse("2023-10-30")));
        assertThat(runOutput.getRow().get("datetime_column"), is(Instant.parse("2023-10-30T23:02:27.150Z")));
        assertThat(runOutput.getRow().get("time_column"), is(LocalTime.parse("14:30:00")));
        assertThat(runOutput.getRow().get("timestamp_column"), is(Instant.parse("2023-10-30T14:30:00.0Z")));
        assertThat(runOutput.getRow().get("year_column"), is(2023));

        assertThat(runOutput.getRow().get("json_column"), is("{\"key\": \"value\"}"));
        assertThat(runOutput.getRow().get("blob_column"), is(Hex.decodeHex("0102030405060708".toCharArray())));
    }

    @Test
    void selectFromExistingDatabase() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URL resource = SqliteTest.class.getClassLoader().getResource("db/Chinook_Sqlite.sqlite");

        URI input = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/file/storage/get.yml"),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        Query task = Query.builder()
            .url(Property.of("jdbc:sqlite:Chinook_Sqlite.sqlite"))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sqliteFile(Property.of(input.toString()))
            .sql(Property.of("""
                SELECT Genre.Name, COUNT(InvoiceLine.InvoiceLineId) AS TracksPurchased
                FROM Genre
                JOIN Track ON Genre.GenreId = Track.GenreId
                JOIN InvoiceLine ON Track.TrackId = InvoiceLine.TrackId
                GROUP BY Genre.Name
                ORDER BY TracksPurchased DESC
                """))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("Name"), is("Rock"));
        assertThat(runOutput.getRow().get("TracksPurchased"), is(835));
    }

    @Test
    void selectFromExistingDatabaseAndOutputDatabase() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URL resource = SqliteTest.class.getClassLoader().getResource("db/Chinook_Sqlite.sqlite");

        URI input = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/file/storage/get.yml"),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        //Fetch and check
        Query task = Query.builder()
            .url(Property.of("jdbc:sqlite:Chinook_Sqlite.sqlite"))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH))
            .timeZoneId(Property.of("Europe/Paris"))
            .sqliteFile(Property.of(input.toString()))
            .sql(Property.of("""
                SELECT * FROM Genre
                """))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRows(), notNullValue());
        assertThat(runOutput.getRows().size(), is(25));

        //Update DB and output file
        Query insert = Query.builder()
            .url(Property.of("jdbc:sqlite:Chinook_Sqlite.sqlite"))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(NONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sqliteFile(Property.of(input.toString()))
            .outputDbFile(Property.of(true))
            .sql(Property.of("""
                INSERT INTO Genre (GenreId, Name) VALUES (26, 'TestInsert');
                """))
            .build();
        Query.Output insertOutput = insert.run(runContext);

        //Check DB size
        //Update DB and output file
        Query check = Query.builder()
            .url(Property.of("jdbc:sqlite:Chinook_Sqlite.sqlite"))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH))
            .timeZoneId(Property.of("Europe/Paris"))
            .sqliteFile(Property.of(insertOutput.getDatabaseUri().toString()))
            .sql(Property.of("""
                SELECT * FROM Genre;
                """))
            .build();
        AbstractJdbcQuery.Output checkOutput = check.run(runContext);
        assertThat(checkOutput.getRows(), notNullValue());
        assertThat(checkOutput.getRows().size(), is(26));
        assertThat(checkOutput.getRows().stream().anyMatch(row -> row.get("Name").equals("TestInsert")), is(true));
    }

    @Test
    void update() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query taskUpdate = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("update lite_types set d = 'D'"))
            .build();

        taskUpdate.run(runContext);

        Query taskGet = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select d from lite_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("d"), is("D"));
    }

    @Override
    protected String getUrl() {
        return TestUtils.url();
    }

    @Override
    protected String getUsername() {
        return TestUtils.username();
    }

    @Override
    protected String getPassword() {
        return TestUtils.password();
    }

    protected Connection getConnection() throws SQLException {
        Properties props = new Properties();
        props.put("jdbc.url", getUrl());
        props.put("user", getUsername());
        props.put("password", getPassword());

        return DriverManager.getConnection(props.getProperty("jdbc.url"), props);
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/sqlite.sql");
    }
}
