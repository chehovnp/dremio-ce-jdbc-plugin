package com.dremio.exec.store.jdbc;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.store.StoragePluginUtils;
import com.dremio.exec.store.jdbc.dialect.JdbcToFieldMapping;
import com.dremio.exec.store.jdbc.dialect.SourceTypeDescriptor;
import com.dremio.exec.store.jdbc.dialect.TableSourceTypeDescriptor;
import com.dremio.exec.store.jdbc.dialect.TypeMapper;
import com.dremio.exec.store.jdbc.proto.JdbcReaderProto;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.ImmutableList.Builder;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDatasetMetadata implements DatasetMetadata, PartitionChunkListing {
   private static final Logger logger = LoggerFactory.getLogger(JdbcDatasetMetadata.class);
   public static final String UNPUSHABLE_KEY = "unpushable";
   public static final String TYPENAME_KEY = "sourceTypeName";
   public static final String EXPLICIT_CAST_KEY = "explicitCast";
   private static final DatasetStats JDBC_STATS;
   private final JdbcSchemaFetcher fetcher;
   private final JdbcSchemaFetcher.JdbcDatasetHandle handle;
   private List<String> skippedColumns;
   private List<JdbcReaderProto.ColumnProperties> columnProperties;
   private List<JdbcToFieldMapping> jdbcToFieldMappings;

   JdbcDatasetMetadata(JdbcSchemaFetcher fetcher, JdbcSchemaFetcher.JdbcDatasetHandle handle) {
      this.fetcher = fetcher;
      this.handle = handle;
   }

   public DatasetStats getDatasetStats() {
      return JDBC_STATS;
   }

   public BytesOutput getExtraInfo() {
      this.buildIfNecessary();
      return (os) -> {
         os.write(JdbcReaderProto.JdbcTableXattr.newBuilder().addAllSkippedColumns(this.skippedColumns).addAllColumnProperties(this.columnProperties).build().toByteArray());
      };
   }

   public Schema getRecordSchema() {
      this.buildIfNecessary();
      return new Schema((Iterable)this.jdbcToFieldMappings.stream().map(JdbcToFieldMapping::getField).collect(Collectors.toList()));
   }

   public Iterator<? extends PartitionChunk> iterator() {
      return Collections.singleton(PartitionChunk.of(new DatasetSplit[]{DatasetSplit.of(9223372036854775807L, this.fetcher.getRowCount(this.handle))})).iterator();
   }

   private void buildIfNecessary() {
      if (this.jdbcToFieldMappings == null) {
         Stopwatch watch = Stopwatch.createStarted();
         if (this.fetcher.usePrepareForColumnMetadata()) {
            this.prepareColumnMetadata();
         } else {
            this.apiColumnMetadata();
         }

         logger.info("Took {} ms to get column metadata for {}", watch.elapsed(TimeUnit.MILLISECONDS), this.handle.getDatasetPath());
      }

   }

   private void apiColumnMetadata() {
      this.getColumnMetadata((connection, filters, skippedColumnBuilder, properties) -> {
         return this.fetcher.config.getDialect().getDataTypeMapper().mapJdbcToArrowFields((sourceTypeDescriptor, shouldSkip) -> {
            this.handleUnpushableColumn(skippedColumnBuilder, properties, sourceTypeDescriptor, shouldSkip);
         }, (colName, propName, propValue) -> {
            this.addColumnProperty(properties, colName, propName, propValue);
         }, connection, filters.getCatalogName(), filters.getSchemaName(), filters.getTableName(), false);
      });
   }

   private void prepareColumnMetadata() {
      this.getColumnMetadata((connection, filters, skippedColumnBuilder, properties) -> {
         String quotedPath = this.fetcher.getQuotedPath(this.handle.getIdentifiers());
         PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + quotedPath);
         Throwable var7 = null;

         List var8;
         try {
            var8 = this.fetcher.config.getDialect().getDataTypeMapper().mapJdbcToArrowFields((sourceTypeDescriptor, shouldSkip) -> {
               this.handleUnpushableColumn(skippedColumnBuilder, properties, sourceTypeDescriptor, shouldSkip);
            }, (colName, propName, propValue) -> {
               this.addColumnProperty(properties, colName, propName, propValue);
            }, (message) -> {
               throw new IllegalArgumentException(message);
            }, connection, filters.getCatalogName(), filters.getSchemaName(), filters.getTableName(), statement.getMetaData(), (Set)null, false, false);
         } catch (Throwable var12) {
            var7 = var12;
            throw var12;
         } finally {
            if (statement != null) {
               $closeResource(var7, statement);
            }

         }

         return var8;
      });
   }

   private void getColumnMetadata(JdbcDatasetMetadata.MapFunction mapFields) {
      try {
         Connection connection = this.fetcher.dataSource.getConnection();
         Throwable var3 = null;

         try {
            DatabaseMetaData metaData = connection.getMetaData();
            JdbcSchemaFetcher.FilterDescriptor filters = new JdbcSchemaFetcher.FilterDescriptor(this.handle.getDatasetPath().getComponents(), JdbcSchemaFetcher.supportsCatalogsWithoutSchemas(this.fetcher.config.getDialect(), metaData));
            Builder<String> skippedColumnBuilder = ImmutableList.builder();
            ListMultimap<String, JdbcReaderProto.ColumnProperty> properties = ArrayListMultimap.create();
            this.jdbcToFieldMappings = mapFields.map(connection, filters, skippedColumnBuilder, properties);
            Builder<JdbcReaderProto.ColumnProperties> columnPropertiesListBuilder = ImmutableList.builder();
            Iterator var9 = properties.keys().iterator();

            while(var9.hasNext()) {
               String colName = (String)var9.next();
               columnPropertiesListBuilder.add(JdbcReaderProto.ColumnProperties.newBuilder().setColumnName(colName).addAllProperties(properties.get(colName)).build());
            }

            this.columnProperties = columnPropertiesListBuilder.build();
            this.skippedColumns = skippedColumnBuilder.build();
         } catch (Throwable var15) {
            var3 = var15;
            throw var15;
         } finally {
            if (connection != null) {
               $closeResource(var3, connection);
            }

         }
      } catch (SQLException var17) {
         throw StoragePluginUtils.message(UserException.dataReadError(var17), this.fetcher.storagePluginName, "Failed getting columns for %s.", new Object[]{this.fetcher.getQuotedPath(this.handle.getDatasetPath().getComponents())}).build(logger);
      }
   }

   private void handleUnpushableColumn(Builder<String> skippedColumnBuilder, ListMultimap<String, JdbcReaderProto.ColumnProperty> properties, SourceTypeDescriptor sourceTypeDescriptor, Boolean shouldSkip) {
      if (shouldSkip) {
         this.addSkippedColumn(skippedColumnBuilder, sourceTypeDescriptor);
      } else {
         this.setColumnNotPushable(properties, sourceTypeDescriptor);
      }

   }

   private void addSkippedColumn(Builder<String> skippedColumnBuilder, SourceTypeDescriptor sourceTypeDescriptor) {
      skippedColumnBuilder.add(sourceTypeDescriptor.getFieldName().toLowerCase(Locale.ROOT));
      this.warnUnsupportedColumnType(sourceTypeDescriptor);
   }

   private void setColumnNotPushable(ListMultimap<String, JdbcReaderProto.ColumnProperty> columnProperties, SourceTypeDescriptor mapping) {
      this.addColumnProperty(columnProperties, mapping.getFieldName(), "unpushable", Boolean.TRUE.toString());
   }

   private void addColumnProperty(ListMultimap<String, JdbcReaderProto.ColumnProperty> columnProperties, String colName, String key, String value) {
      JdbcReaderProto.ColumnProperty colProperty = JdbcReaderProto.ColumnProperty.newBuilder().setKey(key).setValue(value).build();
      columnProperties.put(colName.toLowerCase(Locale.ROOT), colProperty);
   }

   protected void warnUnsupportedColumnType(SourceTypeDescriptor type) {
      TableSourceTypeDescriptor tableDescriptor = (TableSourceTypeDescriptor)type.unwrap(TableSourceTypeDescriptor.class);
      String columnName;
      if (tableDescriptor != null) {
         columnName = String.format("%s.%s.%s.%s", tableDescriptor.getCatalog(), tableDescriptor.getSchema(), tableDescriptor.getTable(), type.getFieldName());
      } else {
         columnName = type.getFieldName();
      }

      logger.warn("A column you queried has a data type that is not currently supported by the JDBC storage plugin. The column's name was {}, its JDBC data type was {}, and the source column type was {}.", new Object[]{columnName, TypeMapper.nameFromType(type.getReportedJdbcType()), type.getDataSourceTypeName()});
   }

   static {
      JDBC_STATS = DatasetStats.of(ScanCostFactor.JDBC.getFactor());
   }

   @FunctionalInterface
   private interface MapFunction {
      List<JdbcToFieldMapping> map(Connection var1, JdbcSchemaFetcher.FilterDescriptor var2, Builder<String> var3, ListMultimap<String, JdbcReaderProto.ColumnProperty> var4) throws SQLException;
   }
}
