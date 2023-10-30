package io.kestra.plugin.jdbc.sqlite;

public abstract class TestUtils {

    public static String url() {
        return "jdbc:sqlite:temp";
    }

    public static String username() {
        return "";
    }

    public static String password() {
        return "";
    }

}
