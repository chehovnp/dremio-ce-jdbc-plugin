package com.dremio.exec.store.jdbc;

import com.dremio.common.AutoCloseables;
import com.dremio.common.dialect.DremioSqlDialect.ContainerSupport;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EmptyDatasetHandleListing;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.store.StoragePluginUtils;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableList.Builder;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSchemaFetcher {
   private static final Logger logger = LoggerFactory.getLogger(JdbcSchemaFetcher.class);
   protected static final long BIG_ROW_COUNT = 1000000000L;
   protected static final Joiner PERIOD_JOINER = Joiner.on(".");
   protected final DataSource dataSource;
   protected final String storagePluginName;
   protected final int timeout;
   protected final JdbcStoragePlugin.Config config;

   public JdbcSchemaFetcher(String name, DataSource dataSource, int timeout, JdbcStoragePlugin.Config config) {
      this.dataSource = dataSource;
      this.storagePluginName = name;
      this.timeout = timeout;
      this.config = config;
   }

   public boolean containerExists(EntityPath containerPath) {
      try {
         List<String> path = containerPath.getComponents();
         if (path.size() == 2) {
            return this.getCatalogsOrSchemas().contains(path.get(1));
         } else if (path.size() == 3) {
            return this.getSchemas((String)path.get(1)).contains(path.get(2));
         } else {
            return path.size() == 1;
         }
      } catch (Exception var3) {
         logger.error("Exception caught while checking if container exists.", var3);
         return false;
      }
   }

   protected long getRowCount(JdbcSchemaFetcher.JdbcDatasetHandle handle) {
      String quotedPath = this.getQuotedPath(handle.getDatasetPath());
      logger.debug("Getting row count for table {}. ", quotedPath);
      Optional<Long> count = this.executeQueryAndGetFirstLong("select count(*) from " + this.getQuotedPath(handle.getIdentifiers()));
      if (count.isPresent()) {
         return (Long)count.get();
      } else {
         logger.debug("There was a problem getting the row count for table {}, using default of {}.", quotedPath, 1000000000L);
         return 1000000000L;
      }
   }

   public DatasetHandleListing getTableHandles() {
      if (this.config.shouldSkipSchemaDiscovery()) {
         logger.debug("Skip schema discovery enabled, skipping getting tables '{}'", this.storagePluginName);
         return new EmptyDatasetHandleListing();
      } else {
         logger.debug("Getting all tables for plugin '{}'", this.storagePluginName);
         return new JdbcSchemaFetcher.JdbcIteratorListing(new JdbcSchemaFetcher.JdbcDatasetMetadataIterable(this.storagePluginName, this.dataSource, this.config));
      }
   }

   public Optional<DatasetHandle> getTableHandle(List<String> tableSchemaPath) {
      try {
         Connection connection = this.dataSource.getConnection();
         Throwable var3 = null;

         Optional var4;
         try {
            if (this.usePrepareForColumnMetadata() && this.config.shouldSkipSchemaDiscovery() || this.usePrepareForGetTables()) {
               var4 = this.getTableHandleViaPrepare(tableSchemaPath, connection);
               return var4;
            }

            var4 = this.getDatasetHandleViaGetTables(tableSchemaPath, connection);
         } catch (Throwable var9) {
            var3 = var9;
            throw var9;
         } finally {
            if (connection != null) {
               $closeResource(var3, connection);
            }

         }

         return var4;
      } catch (SQLException var11) {
         logger.warn("Failed to fetch schema for {}.", tableSchemaPath);
         return Optional.empty();
      }
   }

   public DatasetMetadata getTableMetadata(DatasetHandle datasetHandle) {
      return new JdbcDatasetMetadata(this, (JdbcSchemaFetcher.JdbcDatasetHandle)datasetHandle.unwrap(JdbcSchemaFetcher.JdbcDatasetHandle.class));
   }

   public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle) {
      return new JdbcDatasetMetadata(this, (JdbcSchemaFetcher.JdbcDatasetHandle)datasetHandle.unwrap(JdbcSchemaFetcher.JdbcDatasetHandle.class));
   }

   protected boolean usePrepareForColumnMetadata() {
      return false;
   }

   protected boolean usePrepareForGetTables() {
      return false;
   }

   protected final Optional<Long> executeQueryAndGetFirstLong(String sql) {
      try {
         Connection connection = this.dataSource.getConnection();
         Throwable var3 = null;

         try {
            Statement statement = connection.createStatement();
            Throwable var5 = null;

            try {
               statement.setFetchSize(this.config.getFetchSize());
               statement.setQueryTimeout(this.timeout);
               ResultSet resultSet = statement.executeQuery(sql);
               Throwable var7 = null;

               try {
                  ResultSetMetaData meta = resultSet.getMetaData();
                  int colCount = meta.getColumnCount();
                  Optional numRows;
                  if (colCount == 1 && resultSet.next()) {
                     numRows = resultSet.getLong(1);
                     Optional var12;
                     if (resultSet.wasNull()) {
                        var12 = Optional.empty();
                        return var12;
                     } else {
                        logger.debug("Query `{}` returned {} rows.", sql, Long.valueOf((long)numRows));
                        var12 = Optional.of(Long.valueOf((long)numRows));
                        return var12;
                     }
                  } else {
                     logger.debug("Invalid results returned for `{}`, colCount = {}.", sql, colCount);
                     numRows = Optional.empty();
                     return numRows;
                  }
               } catch (Throwable var37) {
                  var7 = var37;
                  throw var37;
               } finally {
                  if (resultSet != null) {
                     $closeResource(var7, resultSet);
                  }

               }
            } catch (Throwable var39) {
               var5 = var39;
               throw var39;
            } finally {
               if (statement != null) {
                  $closeResource(var5, statement);
               }

            }
         } catch (Throwable var41) {
            var3 = var41;
            throw var41;
         } finally {
            if (connection != null) {
               $closeResource(var3, connection);
            }

         }
      } catch (Exception var43) {
         logger.warn("Took longer than {} seconds to execute query `{}`.", new Object[]{this.timeout, sql, var43});
         return Optional.empty();
      }
   }

   protected static String getJoinedSchema(String catalogName, String schemaName, String tableName) {
      List<String> schemaPathThatFailed = new ArrayList();
      if (!Strings.isNullOrEmpty(catalogName)) {
         schemaPathThatFailed.add(catalogName);
      }

      if (!Strings.isNullOrEmpty(schemaName)) {
         schemaPathThatFailed.add(schemaName);
      }

      if (!Strings.isNullOrEmpty(tableName) && !"%".equals(tableName)) {
         schemaPathThatFailed.add(tableName);
      }

      return PERIOD_JOINER.join(schemaPathThatFailed);
   }

   protected static List<String> getSchemas(DatabaseMetaData metaData, String catalogName, JdbcStoragePlugin.Config config, List<String> failed) {
      Builder<String> builder = ImmutableList.builder();
      logger.debug("Getting schemas for catalog=[{}].", catalogName);

      try {
         ResultSet getSchemasResultSet = Strings.isNullOrEmpty(catalogName) ? metaData.getSchemas() : metaData.getSchemas(catalogName, (String)null);
         Throwable var6 = null;

         try {
            while(getSchemasResultSet.next()) {
               String schema = getSchemasResultSet.getString(1);
               if (!config.getHiddenSchemas().contains(schema)) {
                  builder.add(schema);
               }
            }
         } catch (Throwable var12) {
            var6 = var12;
            throw var12;
         } finally {
            if (getSchemasResultSet != null) {
               $closeResource(var6, getSchemasResultSet);
            }

         }
      } catch (SQLException var14) {
         failed.add(getJoinedSchema(catalogName, (String)null, (String)null));
      }

      return builder.build();
   }

   protected final String getQuotedPath(EntityPath path) {
      return this.getQuotedPath(path.getComponents());
   }

   protected final String getQuotedPath(List<String> tablePath) {
      String[] pathSegments = (String[])tablePath.stream().map((path) -> {
         return this.config.getDialect().quoteIdentifier(path);
      }).toArray((x$0) -> {
         return new String[x$0];
      });
      SchemaPath key = SchemaPath.getCompoundPath(pathSegments);
      return key.getAsUnescapedPath();
   }

   protected static boolean supportsCatalogs(JdbcDremioSqlDialect dialect, DatabaseMetaData metaData) throws SQLException {
      if (dialect.supportsCatalogs() == ContainerSupport.AUTO_DETECT) {
         return !Strings.isNullOrEmpty(metaData.getCatalogTerm());
      } else {
         return dialect.supportsCatalogs() == ContainerSupport.SUPPORTED;
      }
   }

   protected static boolean supportsCatalogsWithoutSchemas(JdbcDremioSqlDialect dialect, DatabaseMetaData metaData) throws SQLException {
      return supportsCatalogs(dialect, metaData) && !supportsSchemas(dialect, metaData);
   }

   protected static boolean supportsSchemas(JdbcDremioSqlDialect dialect, DatabaseMetaData metaData) throws SQLException {
      if (dialect.supportsSchemas() == ContainerSupport.AUTO_DETECT) {
         return !Strings.isNullOrEmpty(metaData.getSchemaTerm());
      } else {
         return dialect.supportsSchemas() == ContainerSupport.SUPPORTED;
      }
   }

   protected static boolean supportsSchemasWithoutCatalogs(JdbcDremioSqlDialect dialect, DatabaseMetaData metaData) throws SQLException {
      return supportsSchemas(dialect, metaData) && !supportsCatalogs(dialect, metaData);
   }

   private List<String> getCatalogsOrSchemas() {
      if (this.config.showOnlyConnDatabase() && this.config.getDatabase() != null) {
         return ImmutableList.of(this.config.getDatabase());
      } else {
         try {
            Connection connection = this.dataSource.getConnection();
            Throwable var2 = null;

            ImmutableList var26;
            try {
               DatabaseMetaData metaData = connection.getMetaData();
               if (supportsSchemasWithoutCatalogs(this.config.getDialect(), metaData)) {
                  List var25 = getSchemas(metaData, (String)null, this.config, new ArrayList());
                  return var25;
               }

               Builder<String> catalogs = ImmutableList.builder();
               logger.debug("Getting catalogs from JDBC source {}", this.storagePluginName);
               ResultSet getCatalogsResultSet = metaData.getCatalogs();
               Throwable var6 = null;

               try {
                  while(getCatalogsResultSet.next()) {
                     catalogs.add(getCatalogsResultSet.getString(1));
                  }
               } catch (Throwable var20) {
                  var6 = var20;
                  throw var20;
               } finally {
                  if (getCatalogsResultSet != null) {
                     $closeResource(var6, getCatalogsResultSet);
                  }

               }

               var26 = catalogs.build();
            } catch (Throwable var22) {
               var2 = var22;
               throw var22;
            } finally {
               if (connection != null) {
                  $closeResource(var2, connection);
               }

            }

            return var26;
         } catch (SQLException var24) {
            logger.error("Error getting catalogs", var24);
            throw new RuntimeException(StoragePluginUtils.generateSourceErrorMessage(this.storagePluginName, "Exception while fetching catalog information."), var24);
         }
      }
   }

   private List<String> getSchemas(String catalogName) {
      try {
         Connection connection = this.dataSource.getConnection();
         Throwable var3 = null;

         List var4;
         try {
            var4 = getSchemas(connection.getMetaData(), catalogName, this.config, new ArrayList());
         } catch (Throwable var9) {
            var3 = var9;
            throw var9;
         } finally {
            if (connection != null) {
               $closeResource(var3, connection);
            }

         }

         return var4;
      } catch (SQLException var11) {
         logger.error("Error getting schemas", var11);
         throw new RuntimeException(StoragePluginUtils.generateSourceErrorMessage(this.storagePluginName, "Exception while fetching schema information."), var11);
      }
   }

   private Optional<DatasetHandle> getDatasetHandleViaGetTables(List<String> tableSchemaPath, Connection connection) throws SQLException {
      DatabaseMetaData metaData = connection.getMetaData();
      JdbcSchemaFetcher.FilterDescriptor filter = new JdbcSchemaFetcher.FilterDescriptor(tableSchemaPath, supportsCatalogsWithoutSchemas(this.config.getDialect(), metaData));
      ResultSet tablesResult = metaData.getTables(filter.getCatalogName(), filter.getSchemaName(), filter.getTableName(), (String[])null);
      Throwable var6 = null;

      Optional var10;
      try {
         String currSchema;
         do {
            if (!tablesResult.next()) {
               return Optional.empty();
            }

            currSchema = tablesResult.getString(2);
         } while(!Strings.isNullOrEmpty(currSchema) && this.config.getHiddenSchemas().contains(currSchema));

         Builder<String> pathBuilder = ImmutableList.builder();
         pathBuilder.add(this.storagePluginName);
         String currCatalog = tablesResult.getString(1);
         if (!Strings.isNullOrEmpty(currCatalog)) {
            pathBuilder.add(currCatalog);
         }

         if (!Strings.isNullOrEmpty(currSchema)) {
            pathBuilder.add(currSchema);
         }

         pathBuilder.add(tablesResult.getString(3));
         var10 = Optional.of(new JdbcSchemaFetcher.JdbcDatasetHandle(new EntityPath(pathBuilder.build())));
      } catch (Throwable var14) {
         var6 = var14;
         throw var14;
      } finally {
         if (tablesResult != null) {
            $closeResource(var6, tablesResult);
         }

      }

      return var10;
   }

   private Optional<DatasetHandle> getTableHandleViaPrepare(List<String> tableSchemaPath, Connection connection) throws SQLException {
      DatabaseMetaData metaData = connection.getMetaData();
      List<String> trimmedList = tableSchemaPath.subList(1, tableSchemaPath.size());
      PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + this.getQuotedPath(trimmedList));
      Throwable var6 = null;

      Optional var10;
      try {
         ResultSetMetaData preparedMetadata = statement.getMetaData();
         if (preparedMetadata.getColumnCount() <= 0) {
            logger.debug("Table has no columns, query is in invalid");
            Optional var16 = Optional.empty();
            return var16;
         }

         Builder<String> pathBuilder = ImmutableList.builder();
         pathBuilder.add(this.storagePluginName);
         String table;
         if (supportsCatalogs(this.config.getDialect(), metaData)) {
            table = preparedMetadata.getCatalogName(1);
            if (!Strings.isNullOrEmpty(table)) {
               pathBuilder.add(table);
            }
         }

         if (supportsSchemas(this.config.getDialect(), metaData)) {
            table = preparedMetadata.getSchemaName(1);
            if (!Strings.isNullOrEmpty(table)) {
               pathBuilder.add(table);
            }
         }

         table = preparedMetadata.getTableName(1);
         if (!Strings.isNullOrEmpty(table)) {
            pathBuilder.add(table);
            var10 = Optional.of(new JdbcSchemaFetcher.JdbcDatasetHandle(new EntityPath(pathBuilder.build())));
            return var10;
         }

         logger.info("Unable to get table handle for {} via prepare, falling back to getTables.", this.getQuotedPath(tableSchemaPath));
         var10 = this.getDatasetHandleViaGetTables(tableSchemaPath, connection);
      } catch (Throwable var14) {
         var6 = var14;
         throw var14;
      } finally {
         if (statement != null) {
            $closeResource(var6, statement);
         }

      }

      return var10;
   }

   private static class JdbcDatasetMetadataIterable extends AbstractIterator<DatasetHandle> implements AutoCloseable {
      private final String storagePluginName;
      private final JdbcStoragePlugin.Config config;
      private Connection connection;
      private DatabaseMetaData metaData;
      private String[] tableTypes;
      private boolean supportsCatalogs;
      private boolean hasConstantSchema;
      private boolean hasErrorDuringRetrieval;
      private final List<String> failedCatalogOrSchema = new ArrayList();
      private Iterator<String> catalogs;
      private String currentCatalog = null;
      private Iterator<String> schemas = null;
      private String currentSchema = null;
      private ResultSet tablesResult = null;

      JdbcDatasetMetadataIterable(String storagePluginName, DataSource dataSource, JdbcStoragePlugin.Config config) {
         this.storagePluginName = storagePluginName;
         this.config = config;
         this.hasErrorDuringRetrieval = false;

         try {
            this.connection = dataSource.getConnection();
            this.metaData = this.connection.getMetaData();
            this.supportsCatalogs = JdbcSchemaFetcher.supportsCatalogs(config.getDialect(), this.metaData);
            if (config.getDatabase() != null && config.showOnlyConnDatabase()) {
               if (this.supportsCatalogs) {
                  this.catalogs = ImmutableList.of(config.getDatabase()).iterator();
               } else if (JdbcSchemaFetcher.supportsSchemasWithoutCatalogs(config.getDialect(), this.metaData)) {
                  this.currentSchema = config.getDatabase();
               }
            }

            if (null == this.catalogs) {
               if (this.supportsCatalogs) {
                  this.catalogs = this.getCatalogs(this.metaData).iterator();
                  if (!this.catalogs.hasNext()) {
                     this.catalogs = Collections.singleton("").iterator();
                  }
               } else {
                  this.catalogs = Collections.singleton((Object)null).iterator();
               }
            }

            this.hasConstantSchema = null != this.currentSchema || !JdbcSchemaFetcher.supportsSchemas(config.getDialect(), this.metaData);
            this.tableTypes = this.getTableTypes(this.metaData);
         } catch (SQLException var5) {
            JdbcSchemaFetcher.logger.error(String.format("Error retrieving all tables for %s", storagePluginName), var5);
            this.catalogs = Collections.emptyIterator();
         }

      }

      protected DatasetHandle computeNext() {
         while(true) {
            if (this.supportsCatalogs && this.currentCatalog == null) {
               if (!this.catalogs.hasNext()) {
                  return this.end();
               }

               this.currentCatalog = (String)this.catalogs.next();
               this.tablesResult = null;
               if (!this.hasConstantSchema) {
                  this.currentSchema = null;
                  this.schemas = null;
               }
            }

            ArrayList path;
            if (!this.hasConstantSchema && this.currentSchema == null) {
               if (this.schemas == null) {
                  path = new ArrayList();
                  this.schemas = JdbcSchemaFetcher.getSchemas(this.metaData, this.currentCatalog, this.config, path).iterator();
                  this.hasErrorDuringRetrieval |= !path.isEmpty();
                  if (this.hasErrorDuringRetrieval && JdbcSchemaFetcher.logger.isDebugEnabled()) {
                     this.failedCatalogOrSchema.addAll(path);
                  }
               }

               if (!this.schemas.hasNext()) {
                  if (!this.supportsCatalogs) {
                     return this.end();
                  }

                  this.currentCatalog = null;
                  continue;
               }

               this.currentSchema = (String)this.schemas.next();
               this.tablesResult = null;
            }

            try {
               if (this.tablesResult == null) {
                  try {
                     this.tablesResult = this.metaData.getTables(this.currentCatalog, this.currentSchema, (String)null, this.tableTypes);
                  } catch (SQLException var4) {
                     this.hasErrorDuringRetrieval = true;
                     if (JdbcSchemaFetcher.logger.isDebugEnabled()) {
                        this.failedCatalogOrSchema.add(JdbcSchemaFetcher.getJoinedSchema(this.currentCatalog, this.currentSchema, (String)null));
                     }

                     if (!this.hasConstantSchema) {
                        this.currentSchema = null;
                        continue;
                     }

                     if (this.supportsCatalogs) {
                        this.currentCatalog = null;
                        continue;
                     }

                     throw var4;
                  }
               }

               if (this.tablesResult.next()) {
                  path = new ArrayList(4);
                  path.add(this.storagePluginName);
                  String currCatalog = this.tablesResult.getString(1);
                  if (!Strings.isNullOrEmpty(currCatalog)) {
                     path.add(currCatalog);
                  }

                  String currSchema = this.tablesResult.getString(2);
                  if (!Strings.isNullOrEmpty(currSchema)) {
                     path.add(currSchema);
                  }

                  path.add(this.tablesResult.getString(3));
                  return new JdbcSchemaFetcher.JdbcDatasetHandle(new EntityPath(path));
               }

               this.tablesResult.close();
               if (this.hasConstantSchema) {
                  if (!this.supportsCatalogs) {
                     return this.end();
                  }

                  this.currentCatalog = null;
               } else {
                  this.currentSchema = null;
               }
            } catch (SQLException var5) {
               JdbcSchemaFetcher.logger.error(String.format("Error listing datasets for '%s'", this.storagePluginName), var5);
               return (DatasetHandle)this.endOfData();
            }
         }
      }

      private DatasetHandle end() {
         JdbcSchemaFetcher.logger.debug("Done fetching all schema and tables for '{}'.", this.storagePluginName);
         if (this.hasErrorDuringRetrieval) {
            if (JdbcSchemaFetcher.logger.isDebugEnabled()) {
               JdbcSchemaFetcher.logger.debug("Failed to fetch schema for {}.", this.failedCatalogOrSchema);
            } else {
               JdbcSchemaFetcher.logger.warn("Failed to fetch some tables, for more information enable debug logging.");
            }
         }

         return (DatasetHandle)this.endOfData();
      }

      private List<String> getCatalogs(DatabaseMetaData metaData) {
         Builder catalogs = ImmutableList.builder();

         try {
            ResultSet getCatalogsResultSet = metaData.getCatalogs();
            Throwable var4 = null;

            try {
               while(getCatalogsResultSet.next()) {
                  catalogs.add(getCatalogsResultSet.getString(1));
               }
            } catch (Throwable var10) {
               var4 = var10;
               throw var10;
            } finally {
               if (getCatalogsResultSet != null) {
                  $closeResource(var4, getCatalogsResultSet);
               }

            }
         } catch (SQLException var12) {
            JdbcSchemaFetcher.logger.error(String.format("Failed to get catalogs for plugin '%s'.", this.storagePluginName), var12);
         }

         return catalogs.build();
      }

      private String[] getTableTypes(DatabaseMetaData metaData) {
         if (this.tableTypes != null) {
            return this.tableTypes;
         } else {
            try {
               ResultSet typesResult = metaData.getTableTypes();
               Throwable var3 = null;

               String[] var13;
               try {
                  ArrayList types = Lists.newArrayList();

                  String type;
                  while(typesResult.next()) {
                     type = typesResult.getString(1).trim();
                     if (!this.config.getHiddenTableTypes().contains(type)) {
                        types.add(type);
                     }
                  }

                  if (types.isEmpty()) {
                     type = null;
                     return type;
                  }

                  var13 = (String[])types.toArray(new String[0]);
               } catch (Throwable var10) {
                  var3 = var10;
                  throw var10;
               } finally {
                  if (typesResult != null) {
                     $closeResource(var3, typesResult);
                  }

               }

               return var13;
            } catch (SQLException var12) {
               JdbcSchemaFetcher.logger.warn("Unable to retrieve list of table types.", var12);
               return null;
            }
         }
      }

      public void close() throws Exception {
         AutoCloseables.close(new AutoCloseable[]{this.tablesResult, this.connection});
      }
   }

   protected static class JdbcIteratorListing<T extends Iterator<DatasetHandle> & AutoCloseable> implements DatasetHandleListing {
      private final T iterator;

      public JdbcIteratorListing(T iterator) {
         this.iterator = iterator;
      }

      public Iterator<DatasetHandle> iterator() {
         return this.iterator;
      }

      public void close() {
         try {
            ((AutoCloseable)this.iterator).close();
         } catch (Exception var2) {
            JdbcSchemaFetcher.logger.warn("Error closing iterator when listing JDBC datasets.", var2);
         }

      }
   }

   protected static class FilterDescriptor {
      private final String catalogName;
      private final String schemaName;
      private final String tableName;

      public FilterDescriptor(List<String> tableSchemaPath, boolean hasCatalogsWithoutSchemas) {
         if (tableSchemaPath.size() == 1 && ((String)tableSchemaPath.get(0)).equals("%")) {
            this.catalogName = null;
            this.schemaName = null;
            this.tableName = "%";
         } else {
            assert tableSchemaPath.size() > 1;

            List<String> tableIdentifier = tableSchemaPath.subList(1, tableSchemaPath.size());
            if (tableIdentifier.size() == 1) {
               this.catalogName = "";
               this.schemaName = "";
               this.tableName = (String)tableIdentifier.get(0);
            } else if (tableIdentifier.size() == 2) {
               this.catalogName = hasCatalogsWithoutSchemas ? (String)tableIdentifier.get(0) : "";
               this.schemaName = hasCatalogsWithoutSchemas ? "" : (String)tableIdentifier.get(0);
               this.tableName = (String)tableIdentifier.get(1);
            } else {
               assert tableIdentifier.size() == 3;

               this.catalogName = (String)tableIdentifier.get(0);
               this.schemaName = (String)tableIdentifier.get(1);
               this.tableName = (String)tableIdentifier.get(2);
            }

         }
      }

      public String getCatalogName() {
         return this.catalogName;
      }

      public String getSchemaName() {
         return this.schemaName;
      }

      public String getTableName() {
         return this.tableName;
      }
   }

   protected static class JdbcDatasetHandle implements DatasetHandle {
      private final EntityPath entityPath;

      public JdbcDatasetHandle(EntityPath entityPath) {
         this.entityPath = entityPath;
      }

      public EntityPath getDatasetPath() {
         return this.entityPath;
      }

      public List<String> getIdentifiers() {
         List<String> components = this.entityPath.getComponents();
         return components.subList(1, components.size());
      }
   }
}
