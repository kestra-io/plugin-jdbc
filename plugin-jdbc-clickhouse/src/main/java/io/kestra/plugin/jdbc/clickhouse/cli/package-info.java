@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for using ClickHouse-local.\n " +
	    "ClickHouse-local is an easy-to-use version of ClickHouse that is ideal for developers who need to perform fast processing on local and remote files using SQL",
    categories = {PluginSubGroup.PluginCategory.DATABASE, PluginSubGroup.PluginCategory.TOOL}
)
package io.kestra.plugin.jdbc.clickhouse.cli;

import io.kestra.core.models.annotations.PluginSubGroup;