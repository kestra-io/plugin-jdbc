@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for accessing the AS400 database.",
    categories = PluginSubGroup.PluginCategory.DATABASE,
    categories = {
        PluginSubGroup.PluginCategory.DATA,
        PluginSubGroup.PluginCategory.INFRASTRUCTURE
    }
)
package io.kestra.plugin.jdbc.as400;

import io.kestra.core.models.annotations.PluginSubGroup;