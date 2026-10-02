package io.kestra.plugin.jdbc.mysql;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

class MySqlConnectionPropertiesTest {
    private static final MySqlConnectionInterface MYSQL = new MySqlConnectionInterface() {
        @Override
        public io.kestra.core.models.property.Property<String> getUrl() {
            return null;
        }

        @Override
        public io.kestra.core.models.property.Property<String> getUsername() {
            return null;
        }

        @Override
        public io.kestra.core.models.property.Property<String> getPassword() {
            return null;
        }

        @Override
        public void registerDriver() {
        }
    };

    private static Properties props() {
        var props = new Properties();
        props.put("jdbc.url", "jdbc:mysql://127.0.0.1:3306/kestra");
        return props;
    }

    @Test
    void urlIsStableAcrossRunsWithoutInputFile() {
        var first = MYSQL.createMysqlProperties(props(), null, false).getProperty("jdbc.url");
        var second = MYSQL.createMysqlProperties(props(), null, false).getProperty("jdbc.url");

        assertThat(first, is(second));
        assertThat(first, not(containsString("allowLoadLocalInfileInPath")));
        assertThat(first, containsString("allowLoadLocalInfile=false"));
        assertThat(first, containsString("useCursorFetch=true"));
    }

    @Test
    void urlContainsWorkingDirWithInputFile() {
        var url = MYSQL.createMysqlProperties(props(), Path.of("/tmp/run-1"), false).getProperty("jdbc.url");

        assertThat(url, containsString("allowLoadLocalInfileInPath"));
        assertThat(url, containsString("run-1"));
        assertThat(url, containsString("allowLoadLocalInfile=false"));
    }
}
