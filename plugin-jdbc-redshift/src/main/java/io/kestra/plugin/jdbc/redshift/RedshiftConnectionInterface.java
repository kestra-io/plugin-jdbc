package io.kestra.plugin.jdbc.redshift;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.lang3.StringUtils;

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
        description = """
            Defaults to `false`. When true, the driver exchanges AWS credentials for temporary database credentials (`GetClusterCredentials` for provisioned clusters, `GetCredentials` for Serverless).
            It is enabled automatically when `accessKeyId` is set. Without `accessKeyId`, the driver uses the default AWS credentials provider chain of the worker (environment variables, instance profile, IRSA).
            Cannot be combined with a non-empty `password`.
            """
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
        title = "AWS secret access key for IAM authentication",
        description = "Required when `accessKeyId` is set."
    )
    @PluginProperty(group = "connection", secret = true)
    Property<String> getSecretKeyId();

    @Schema(
        title = "AWS session token for temporary credentials",
        description = "Used only with `accessKeyId`."
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
        title = "Create the database user if it does not exist (provisioned clusters)",
        description = "Defaults to `false`."
    )
    @PluginProperty(group = "advanced")
    Property<Boolean> getAutoCreate();

    @Schema(
        title = "Redshift Serverless workgroup name",
        description = "Set it to authenticate against Redshift Serverless. Leave it empty for a provisioned cluster."
    )
    @PluginProperty(group = "connection")
    Property<String> getWorkgroupName();

    @Override
    default Properties connectionProperties(RunContext runContext) throws Exception {
        var props = JdbcConnectionInterface.super.connectionProperties(runContext);
        var rAccessKeyId = runContext.render(this.getAccessKeyId()).as(String.class).orElse(null);
        var rIamAuth = runContext.render(this.getIamAuth()).as(Boolean.class).orElse(false);
        if (rAccessKeyId == null && !rIamAuth) {
            return props;
        }

        if (StringUtils.isNotBlank(props.getProperty("password"))) {
            throw new IllegalArgumentException("Set either `password` or IAM authentication (`iamAuth`, `accessKeyId`), not both.");
        }

        var url = props.getProperty("jdbc.url");
        if (url.startsWith(SCHEME)) {
            props.setProperty("jdbc.url", "%s%s".formatted(IAM_SCHEME, url.substring(SCHEME.length())));
        } else if (!url.startsWith(IAM_SCHEME)) {
            throw new IllegalArgumentException("IAM authentication needs a `jdbc:redshift://` or `jdbc:redshift:iam://` URL.");
        }

        // The driver gets temporary credentials from IAM, so no password is sent and `username` only serves as the default `dbUser`.
        var username = (String) props.remove("user");
        props.remove("password");

        if (rAccessKeyId != null) {
            var rSecretKeyId = runContext.render(this.getSecretKeyId()).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("`secretKeyId` is required when `accessKeyId` is set."));
            props.setProperty("AccessKeyID", rAccessKeyId);
            props.setProperty("SecretAccessKey", rSecretKeyId);
            runContext.render(this.getSessionToken()).as(String.class).ifPresent(rSessionToken -> props.setProperty("SessionToken", rSessionToken));
        }
        runContext.render(this.getRegion()).as(String.class).ifPresent(rRegion -> props.setProperty("Region", rRegion));

        var rWorkgroupName = runContext.render(this.getWorkgroupName()).as(String.class).orElse(null);
        if (rWorkgroupName != null) {
            props.setProperty("isServerless", "true");
            props.setProperty("serverlessWorkGroup", rWorkgroupName);
        } else {
            var rDbUser = runContext.render(this.getDbUser()).as(String.class).orElse(username);
            if (rDbUser != null) {
                props.setProperty("DbUser", rDbUser);
            }
            runContext.render(this.getAutoCreate()).as(Boolean.class)
                .ifPresent(rAutoCreate -> props.setProperty("AutoCreate", rAutoCreate.toString()));
        }

        return props;
    }
}
