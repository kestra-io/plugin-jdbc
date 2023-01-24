package io.kestra.plugin.jdbc.rockset;

import io.kestra.core.models.annotations.PluginProperty;

public interface RocksetConnection {
    @PluginProperty(dynamic = true)
    String getApiKey();

    @PluginProperty(dynamic = true)
    String getApiServer();
}
