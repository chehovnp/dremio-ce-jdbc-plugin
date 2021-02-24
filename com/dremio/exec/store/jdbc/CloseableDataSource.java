package com.dremio.exec.store.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;

public interface CloseableDataSource extends DataSource, AutoCloseable {
   static <DS extends DataSource & AutoCloseable> CloseableDataSource wrap(DS datasource) {
      return (CloseableDataSource)(datasource instanceof CloseableDataSource ? (CloseableDataSource)datasource : new CloseableDataSource.DatasourceWrapper(datasource));
   }

   public static final class DatasourceWrapper<DS extends DataSource & AutoCloseable> implements CloseableDataSource {
      private final DS datasource;

      private DatasourceWrapper(DS datasource) {
         this.datasource = datasource;
      }

      public void close() throws Exception {
         ((AutoCloseable)this.datasource).close();
      }

      public <T> T unwrap(Class<T> iface) throws SQLException {
         return this.datasource.unwrap(iface);
      }

      public boolean isWrapperFor(Class<?> iface) throws SQLException {
         return this.datasource.isWrapperFor(iface);
      }

      public void setLoginTimeout(int seconds) throws SQLException {
         this.datasource.setLoginTimeout(seconds);
      }

      public void setLogWriter(PrintWriter out) throws SQLException {
         this.datasource.setLogWriter(out);
      }

      public Logger getParentLogger() throws SQLFeatureNotSupportedException {
         return this.datasource.getParentLogger();
      }

      public int getLoginTimeout() throws SQLException {
         return this.datasource.getLoginTimeout();
      }

      public PrintWriter getLogWriter() throws SQLException {
         return this.datasource.getLogWriter();
      }

      public Connection getConnection(String username, String password) throws SQLException {
         return this.datasource.getConnection(username, password);
      }

      public Connection getConnection() throws SQLException {
         return this.datasource.getConnection();
      }
   }

   @FunctionalInterface
   public interface Factory {
      CloseableDataSource newDataSource() throws SQLException;
   }
}
