package io.kestra.plugin.jdbc.hana;

import org.junit.jupiter.api.Assumptions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

@Testcontainers
public abstract class HanaTestUtils {

    protected static final String USERNAME = "SYSTEM";
    protected static final String PASSWORD = "HXEHana1";

    @Container
    protected static final GenericContainer<?> HANA =
        new GenericContainer<>("saplabs/hanaexpress:latest")
        
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("hana"))

            // HANA SQL port
            .withExposedPorts(39017)

            // HANA is SLOW
            .withStartupTimeout(Duration.ofMinutes(15))

            // Don't retry endlessly
            .withStartupAttempts(1);

    protected static String getJdbcUrl() {
        Assumptions.assumeTrue(
            HANA.isRunning(),
            "SAP HANA container did not start (skipping heavy integration tests)"
        );

        return "jdbc:sap://" + HANA.getHost() + ":" + HANA.getMappedPort(39017);
    }
}
