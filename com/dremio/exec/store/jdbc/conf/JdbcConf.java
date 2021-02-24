package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import javax.inject.Provider;

public abstract class JdbcConf<T extends DialectConf<T, JdbcStoragePlugin>> extends DialectConf<T, JdbcStoragePlugin> {
   protected static final String ENABLE_EXTERNAL_QUERY_LABEL = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)";

   public JdbcConf() {
   }

   protected abstract JdbcStoragePlugin.Config toPluginConfig(SabotContext var1);

   public JdbcStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      return new JdbcStoragePlugin(this.toPluginConfig(context), context, name, pluginIdProvider);
   }
}
