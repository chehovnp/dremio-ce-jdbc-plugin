package com.dremio.exec.store.jdbc.legacy;

import com.dremio.common.dialect.DremioSqlDialect;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.dialect.AutomaticTypeMapper;
import com.dremio.exec.store.jdbc.dialect.TypeMapper;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverter;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverterBase;
import javax.sql.DataSource;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;

public class JdbcDremioSqlDialect extends DremioSqlDialect {
   public static final JdbcDremioSqlDialect DERBY;

   public JdbcDremioRelToSqlConverter getConverter() {
      return new JdbcDremioRelToSqlConverterBase(this);
   }

   protected JdbcDremioSqlDialect(String databaseProductName, String identifierQuoteString, NullCollation nullCollation) {
      super(databaseProductName, identifierQuoteString, nullCollation);
   }

   public JdbcSchemaFetcher getSchemaFetcher(String name, DataSource dataSource, int timeout, JdbcStoragePlugin.Config config) {
      return new JdbcSchemaFetcher(name, dataSource, timeout, config);
   }

   public TypeMapper getDataTypeMapper() {
      return AutomaticTypeMapper.INSTANCE;
   }

   static {
      DERBY = new JdbcDremioSqlDialect(DatabaseProduct.DERBY.name(), "\"", NullCollation.HIGH);
   }
}
