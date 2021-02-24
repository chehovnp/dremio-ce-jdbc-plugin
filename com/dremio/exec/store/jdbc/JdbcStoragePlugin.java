package com.dremio.exec.store.jdbc;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.StoragePluginUtils;
import com.dremio.exec.store.SupportsExternalQuery;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.legacy.LegacyDialect;
import com.dremio.exec.store.jdbc.rel.JdbcPrel;
import com.dremio.exec.tablefunctions.DremioCalciteResource;
import com.dremio.exec.tablefunctions.ExternalQueryScanPrel;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.BooleanCapability;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.CapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.inject.Provider;
import javax.sql.DataSource;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcStoragePlugin implements StoragePlugin, SourceMetadata, SupportsListingDatasets, SupportsExternalQuery {
   private static final Logger LOGGER = LoggerFactory.getLogger(JdbcStoragePlugin.class);
   public static final BooleanCapability REQUIRE_TRIMS_ON_CHARS;
   public static final BooleanCapability COERCE_TIMES_TO_UTC;
   public static final BooleanCapability COERCE_TIMESTAMPS_TO_UTC;
   public static final BooleanCapability ADJUST_DATE_TIMEZONE;
   private final String name;
   private final JdbcStoragePlugin.Config config;
   private final Provider<StoragePluginId> pluginIdProvider;
   private final SabotContext context;
   private final int timeout;
   private JdbcDremioSqlDialect dialect;
   private JdbcSchemaFetcher fetcher;
   private CloseableDataSource source;

   public JdbcStoragePlugin(JdbcStoragePlugin.Config config, SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      this.name = name;
      this.config = config;
      this.pluginIdProvider = pluginIdProvider;
      this.context = context;
      this.dialect = config.getDialect();
      this.timeout = (int)context.getOptionManager().getOption(ExecConstants.JDBC_ROW_COUNT_QUERY_TIMEOUT_VALIDATOR);
   }

   public boolean containerExists(EntityPath containerPath) {
      return this.fetcher.containerExists(containerPath);
   }

   public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
      return this.fetcher.getTableHandles();
   }

   public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) {
      return datasetPath.size() >= 2 && datasetPath.size() <= 4 ? this.fetcher.getTableHandle(datasetPath.getComponents()) : Optional.empty();
   }

   public DatasetMetadata getDatasetMetadata(DatasetHandle datasetHandle, PartitionChunkListing chunkListing, GetMetadataOption... options) {
      return this.fetcher.getTableMetadata(datasetHandle);
   }

   public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options) {
      return this.fetcher.listPartitionChunks(datasetHandle);
   }

   public SourceCapabilities getSourceCapabilities() {
      return this.dialect == null ? new SourceCapabilities(new CapabilityValue[0]) : new SourceCapabilities(new CapabilityValue[]{new BooleanCapabilityValue(SourceCapabilities.TREAT_CALCITE_SCAN_COST_AS_INFINITE, true), new BooleanCapabilityValue(SourceCapabilities.SUBQUERY_PUSHDOWNABLE, this.dialect.supportsSubquery()), new BooleanCapabilityValue(SourceCapabilities.CORRELATED_SUBQUERY_PUSHDOWN, this.dialect.supportsCorrelatedSubquery()), new BooleanCapabilityValue(REQUIRE_TRIMS_ON_CHARS, this.dialect.requiresTrimOnChars()), new BooleanCapabilityValue(COERCE_TIMES_TO_UTC, this.dialect.coerceTimesToUTC()), new BooleanCapabilityValue(COERCE_TIMESTAMPS_TO_UTC, this.dialect.coerceTimestampsToUTC()), new BooleanCapabilityValue(ADJUST_DATE_TIMEZONE, this.dialect.adjustDateTimezone())});
   }

   public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
      return this.context.getConfig().getClass("dremio.plugins.jdbc.rulesfactory", StoragePluginRulesFactory.class, JdbcRulesFactory.class);
   }

   public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
      return true;
   }

   public SourceState getState() {
      if (null == this.source) {
         LOGGER.error("JDBC source {} has not been started.", this.name);
         return SourceState.badState(new String[]{String.format("JDBC source %s has not been started.", this.name)});
      } else {
         try {
            Connection connection = this.source.getConnection();
            Throwable var2 = null;

            SourceState var4;
            try {
               boolean isValid = connection.isValid(1);
               if (isValid) {
                  var4 = SourceState.GOOD;
                  return var4;
               }

               var4 = SourceState.badState(new String[]{"Connection is not valid."});
            } catch (Throwable var15) {
               var2 = var15;
               throw var15;
            } finally {
               if (connection != null) {
                  if (var2 != null) {
                     try {
                        connection.close();
                     } catch (Throwable var14) {
                        var2.addSuppressed(var14);
                     }
                  } else {
                     connection.close();
                  }
               }

            }

            return var4;
         } catch (Exception var17) {
            LOGGER.error("Connection is not valid.", var17);
            return SourceState.badState(var17);
         }
      }
   }

   /** @deprecated */
   @Deprecated
   public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
      return null;
   }

   public void start() throws IOException {
      if (this.dialect == null) {
         throw new RuntimeException("Failure instantiating the dialect for this source. Please see Dremio logs for more information.");
      } else {
         try {
            this.source = this.config.getDatasourceFactory().newDataSource();
         } catch (SQLException var2) {
            throw new IOException(StoragePluginUtils.generateSourceErrorMessage(this.name, var2.getMessage()), var2);
         }

         this.fetcher = this.config.getDialect().getSchemaFetcher(this.name, this.source, this.timeout, this.config);
      }
   }

   public DataSource getSource() {
      return this.source;
   }

   public JdbcStoragePlugin.Config getConfig() {
      return this.config;
   }

   public String getName() {
      return this.name;
   }

   public JdbcDremioSqlDialect getDialect() {
      return this.dialect;
   }

   private StoragePluginId getPluginId() {
      return (StoragePluginId)this.pluginIdProvider.get();
   }

   public void close() throws Exception {
      AutoCloseables.close(new AutoCloseable[]{this.source});
   }

   public List<Function> getFunctions(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
      String sourceName = this.getPluginId().getName();
      if (this.config.allowExternalQuery) {
         return (List)SupportsExternalQuery.getExternalQueryFunction((query) -> {
            return JdbcExternalQueryMetadataUtility.getBatchSchema(this.source, this.dialect, query, sourceName);
         }, (schema) -> {
            return CalciteArrowHelper.wrap(schema).toCalciteRecordType(SqlTypeFactoryImpl.INSTANCE, (f) -> {
               return f.getType().getTypeID() != Null.TYPE_TYPE;
            });
         }, this.getPluginId(), tableSchemaPath).map(Collections::singletonList).orElse(Collections.emptyList());
      } else {
         String errorMsg = this.dialect instanceof LegacyDialect ? "External Query is not supported with legacy mode enabled on source" : "Permission denied to run External Query on source";
         throw newValidationError(errorMsg, sourceName);
      }
   }

   private static CalciteContextException newValidationError(String errorMsg, String sourceName) {
      return SqlUtil.newContextException(SqlParserPos.ZERO, DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryNotSupportedError(errorMsg + " <" + sourceName + ">"));
   }

   public PhysicalOperator getExternalQueryPhysicalOperator(PhysicalPlanCreator creator, ExternalQueryScanPrel prel, BatchSchema schema, String sql) {
      SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
      com.google.common.collect.ImmutableSet.Builder<String> skippedColumnsBuilder = new com.google.common.collect.ImmutableSet.Builder();
      this.filterBatchSchema(schema, schemaBuilder, skippedColumnsBuilder);
      BatchSchema filteredSchema = schemaBuilder.build();
      ImmutableSet<String> skippedColumns = skippedColumnsBuilder.build();
      return new JdbcGroupScan(creator.props(prel, "$dremio$", schema, JdbcPrel.RESERVE, JdbcPrel.LIMIT), sql, (List)filteredSchema.getFields().stream().map((f) -> {
         return SchemaPath.getSimplePath(f.getName());
      }).collect(ImmutableList.toImmutableList()), this.getPluginId(), filteredSchema, skippedColumns);
   }

   private void filterBatchSchema(BatchSchema originalSchema, SchemaBuilder filteredSchemaBuilder, com.google.common.collect.ImmutableSet.Builder<String> skippedColumnsBuilder) {
      Iterator var4 = originalSchema.iterator();

      while(var4.hasNext()) {
         Field field = (Field)var4.next();
         if (field.getType().getTypeID() == Null.TYPE_TYPE) {
            skippedColumnsBuilder.add(field.getName());
         } else {
            filteredSchemaBuilder.addField(field);
         }
      }

   }

   static {
      DriverManager.getDrivers();
      REQUIRE_TRIMS_ON_CHARS = new BooleanCapability("require_trims_on_chars", false);
      COERCE_TIMES_TO_UTC = new BooleanCapability("coerce_times_to_utc", false);
      COERCE_TIMESTAMPS_TO_UTC = new BooleanCapability("coerce_timestamps_to_utc", false);
      ADJUST_DATE_TIMEZONE = new BooleanCapability("adjust_date_timezone", false);
   }

   public static final class Config {
      private final JdbcDremioSqlDialect dialect;
      private final int fetchSize;
      private final CloseableDataSource.Factory datasourceFactory;
      private final String database;
      private final boolean showOnlyConnDatabase;
      private final Set<String> hiddenSchemas;
      private final Set<String> hiddenTableTypes;
      private final boolean skipSchemaDiscovery;
      private final boolean usePrepareForGetTables;
      private final boolean allowExternalQuery;

      private Config(JdbcStoragePlugin.Config.Builder builder) {
         this.dialect = (JdbcDremioSqlDialect)Preconditions.checkNotNull(builder.dialect);
         this.datasourceFactory = (CloseableDataSource.Factory)Preconditions.checkNotNull(builder.datasourceFactory);
         this.fetchSize = builder.fetchSize;
         this.database = builder.database;
         this.showOnlyConnDatabase = builder.showOnlyConnDatabase;
         this.hiddenSchemas = unmodifiableCopyOf(builder.hiddenSchemas);
         this.hiddenTableTypes = unmodifiableCopyOf(builder.hiddenTableTypes);
         this.skipSchemaDiscovery = builder.skipSchemaDiscovery;
         this.usePrepareForGetTables = builder.usePrepareForGetTables;
         this.allowExternalQuery = builder.allowExternalQuery;
      }

      private static Set<String> unmodifiableCopyOf(Set<String> set) {
         Set<String> result = new TreeSet(String.CASE_INSENSITIVE_ORDER);
         result.addAll(set);
         return Collections.unmodifiableSet(result);
      }

      public static JdbcStoragePlugin.Config.Builder newBuilder() {
         return new JdbcStoragePlugin.Config.Builder();
      }

      public JdbcDremioSqlDialect getDialect() {
         return this.dialect;
      }

      public int getFetchSize() {
         return this.fetchSize;
      }

      public CloseableDataSource.Factory getDatasourceFactory() {
         return this.datasourceFactory;
      }

      public String getDatabase() {
         return this.database;
      }

      public boolean showOnlyConnDatabase() {
         return this.showOnlyConnDatabase;
      }

      public Set<String> getHiddenSchemas() {
         return this.hiddenSchemas;
      }

      public Set<String> getHiddenTableTypes() {
         return this.hiddenTableTypes;
      }

      public boolean shouldSkipSchemaDiscovery() {
         return this.skipSchemaDiscovery;
      }

      public boolean usePrepareForGetTables() {
         return this.usePrepareForGetTables;
      }

      public boolean allowExternalQuery() {
         return this.allowExternalQuery;
      }

      public static final class Builder {
         public static final Set<String> DEFAULT_HIDDEN_SCHEMAS = newUnmodifiableSet("SYS", "INFORMATION_SCHEMA");
         public static final Set<String> DEFAULT_HIDDEN_TABLE_TYPES = newUnmodifiableSet("INDEX", "SEQUENCE", "SYSTEM INDEX", "SYSTEM VIEW", "SYSTEM TOAST INDEX", "SYSTEM TOAST TABLE", "SYSTEM TABLE", "TEMPORARY TABLE", "TEMPORARY VIEW", "TYPE");
         private JdbcDremioSqlDialect dialect;
         private int fetchSize;
         private String database;
         private boolean showOnlyConnDatabase;
         private CloseableDataSource.Factory datasourceFactory;
         private final Set<String> hiddenSchemas;
         private final Set<String> hiddenTableTypes;
         private boolean skipSchemaDiscovery;
         private boolean usePrepareForGetTables;
         private boolean allowExternalQuery;

         private static Set<String> newUnmodifiableSet(String... strings) {
            return Collections.unmodifiableSet((Set)Arrays.stream(strings).collect(Collectors.toCollection(() -> {
               return new TreeSet(String.CASE_INSENSITIVE_ORDER);
            })));
         }

         private Builder() {
            this.hiddenSchemas = new TreeSet(String.CASE_INSENSITIVE_ORDER);
            this.hiddenTableTypes = new TreeSet(String.CASE_INSENSITIVE_ORDER);
            this.allowExternalQuery = false;
            this.hiddenSchemas.addAll(DEFAULT_HIDDEN_SCHEMAS);
            this.hiddenTableTypes.addAll(DEFAULT_HIDDEN_TABLE_TYPES);
         }

         public JdbcStoragePlugin.Config.Builder withDialect(JdbcDremioSqlDialect dialect) {
            this.dialect = (JdbcDremioSqlDialect)Preconditions.checkNotNull(dialect);
            return this;
         }

         public JdbcStoragePlugin.Config.Builder withFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
         }

         public JdbcStoragePlugin.Config.Builder withDatabase(String database) {
            this.database = database;
            return this;
         }

         public JdbcStoragePlugin.Config.Builder withShowOnlyConnDatabase(boolean showOnlyConnDatabase) {
            this.showOnlyConnDatabase = showOnlyConnDatabase;
            return this;
         }

         public JdbcStoragePlugin.Config.Builder withDatasourceFactory(CloseableDataSource.Factory factory) {
            this.datasourceFactory = factory;
            return this;
         }

         public JdbcStoragePlugin.Config.Builder addHiddenSchema(String schema, String... others) {
            this.hiddenSchemas.add(schema);
            if (others != null) {
               this.hiddenSchemas.addAll(Arrays.asList(others));
            }

            return this;
         }

         public JdbcStoragePlugin.Config.Builder clearHiddenSchemas() {
            this.hiddenSchemas.clear();
            return this;
         }

         public JdbcStoragePlugin.Config.Builder addHiddenTableType(String tableType, String... others) {
            this.hiddenTableTypes.add(tableType);
            if (others != null) {
               this.hiddenSchemas.addAll(Arrays.asList(others));
            }

            return this;
         }

         public JdbcStoragePlugin.Config.Builder clearHiddenTableTypes() {
            this.hiddenTableTypes.clear();
            return this;
         }

         public JdbcStoragePlugin.Config.Builder withSkipSchemaDiscovery(boolean skipSchemaDiscovery) {
            this.skipSchemaDiscovery = skipSchemaDiscovery;
            return this;
         }

         public JdbcStoragePlugin.Config.Builder withPrepareForGetTables(boolean prepareForGetTables) {
            this.usePrepareForGetTables = prepareForGetTables;
            return this;
         }

         public JdbcStoragePlugin.Config.Builder withAllowExternalQuery(boolean allowExternalQuery) {
            this.allowExternalQuery = allowExternalQuery;
            return this;
         }

         public JdbcStoragePlugin.Config build() {
            return new JdbcStoragePlugin.Config(this);
         }
      }
   }
}
