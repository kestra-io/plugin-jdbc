package io.kestra.plugin.jdbc.redshift;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Properties;

public interface RedshiftConnectionInterface extends JdbcConnectionInterface {
    String SCHEME = "jdbc:redshift://";
    String IAM_SCHEME = "jdbc:redshift:iam://";

    @Override
    default String getScheme() {
        return "jdbc:redshift";
    }

    @Schema(
        title = "Authenticate with AWS IAM instead of a database password",
        description = "When true, the driver exchanges AWS credentials for temporary database credentials (`GetClusterCredentials` for provisioned clusters, `GetCredentials` for Serverless). " +
            "It is enabled automatically when `accessKeyId` is set. Without `accessKeyId`, the driver uses the default AWS credentials provider chain (environment variables, instance profile, IRSA, ...)."
    )
    @PluginProperty(group = "connection")
    Property<Boolean> getIamAuth();

    @Schema(
        title = "AWS access key ID for IAM authentication",
        description = "Setting it enables IAM authentication. Leave it empty to use the default AWS credentials provider chain with `iamAuth: true`."
    )
    @PluginProperty(group = "connection", secret = true)
    Property<String> getAccessKeyId();

    @Schema(
        title = "AWS secret access key for IAM authentication"
    )
    @PluginProperty(group = "connection", secret = true)
    Property<String> getSecretKeyId();

    @Schema(
        title = "AWS session token for temporary credentials"
    )
    @PluginProperty(group = "connection", secret = true)
    Property<String> getSessionToken();

    @Schema(
        title = "AWS region of the cluster or workgroup",
        description = "Required by the driver when the URL host does not contain the region."
    )
    @PluginProperty(group = "connection")
    Property<String> getRegion();

    @Schema(
        title = "Database user to get temporary credentials for (provisioned clusters)",
        description = "Defaults to `username` when it is set. Not used for Redshift Serverless, where the database user is derived from the IAM identity."
    )
    @PluginProperty(group = "connection")
    Property<String> getDbUser();

    @Schema(
        title = "Create the database user if it does not exist (provisioned clusters)"
    )
    @PluginProperty(group = "advanced")
    Property<Boolean> getAutoCreate();

    @Schema(
        title = "Redshift Serverless workgroup name",
        description = "Set it to authenticate against Redshift Serverless; leave it empty for a provisioned cluster."
    )
    @PluginProperty(group = "connection")
    Property<String> getWorkgroupName();

    @Override
    default Properties connectionProperties(RunContext runContext) throws Exception {
        Properties props = JdbcConnectionInterface.super.connectionProperties(runContext);
        String accessKeyId = render(runContext, this.getAccessKeyId());
        boolean iam = accessKeyId != null || runContext.render(this.getIamAuth()).as(Boolean.class).orElse(false);
        if (!iam) {
            return props;
        }

        String url = props.getProperty("jdbc.url");
        if (url.startsWith(SCHEME)) {
            props.setProperty("jdbc.url", IAM_SCHEME + url.substring(SCHEME.length()));
        } else if (!url.startsWith(IAM_SCHEME)) {
            throw new IllegalArgumentException("IAM authentication needs a `jdbc:redshift://` or `jdbc:redshift:iam://` URL.");
        }

        // The driver gets temporary credentials from IAM, so no database password is sent.
        String username = (String) props.remove("user");
        props.remove("password");

        if (accessKeyId != null) {
            String secretKey = render(runContext, this.getSecretKeyId());
            if (secretKey == null) {
                throw new IllegalArgumentException("`secretKeyId` is required when `accessKeyId` is set.");
            }
            props.setProperty("AccessKeyID", accessKeyId);
            props.setProperty("SecretAccessKey", secretKey);
            putIfPresent(props, "SessionToken", render(runContext, this.getSessionToken()));
        }
        putIfPresent(props, "Region", render(runContext, this.getRegion()));

        String workgroup = render(runContext, this.getWorkgroupName());
        if (workgroup != null) {
            props.setProperty("isServerless", "true");
            props.setProperty("serverlessWorkGroup", workgroup);
        } else {
            String dbUser = render(runContext, this.getDbUser());
            putIfPresent(props, "DbUser", dbUser != null ? dbUser : username);
            runContext.render(this.getAutoCreate()).as(Boolean.class)
                .ifPresent(autoCreate -> props.setProperty("AutoCreate", autoCreate.toString()));
        }

        return props;
    }

    private static String render(RunContext runContext, Property<String> property) throws IllegalVariableEvaluationException {
        String value = runContext.render(property).as(String.class).orElse(null);
        return value == null || value.isBlank() ? null : value;
    }

    private static void putIfPresent(Properties props, String key, String value) {
        if (value != null) {
            props.setProperty(key, value);
        }
    }
}
