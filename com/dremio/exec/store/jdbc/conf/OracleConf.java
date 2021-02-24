package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.dialect.OracleDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.legacy.LegacyCapableJdbcConf;
import com.dremio.exec.store.jdbc.legacy.LegacyDialect;
import com.dremio.exec.store.jdbc.legacy.OracleLegacyDialect;
import com.dremio.security.CredentialsService;
import com.dremio.security.PasswordCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.protostuff.Tag;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;
import javax.security.auth.x500.X500Principal;
import javax.sql.ConnectionPoolDataSource;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.reflect.MethodUtils;

@SourceType(
   value = "ORACLE",
   label = "Oracle",
   uiConfig = "oracle-layout.json"
)
public class OracleConf extends LegacyCapableJdbcConf<OracleConf> {
   private static final String ARP_FILENAME = "arp/implementation/oracle-arp.yaml";
   private static final OracleDialect ORACLE_ARP_DIALECT = (OracleDialect)AbstractArpConf.loadArpFile("arp/implementation/oracle-arp.yaml", OracleDialect::new);
   private static final String POOLED_DATASOURCE = "oracle.jdbc.pool.OracleConnectionPoolDataSource";
   @NotBlank
   @Tag(1)
   @DisplayMetadata(
      label = "Host"
   )
   public String hostname;
   @NotBlank
   @Tag(2)
   @Min(1L)
   @Max(65535L)
   @DisplayMetadata(
      label = "Port"
   )
   public String port = "1521";
   @NotBlank
   @Tag(3)
   @DisplayMetadata(
      label = "Service Name"
   )
   public String instance;
   @Tag(4)
   public String username;
   @Tag(5)
   @Secret
   public String password;
   @Tag(6)
   public AuthenticationType authenticationType;
   @Tag(7)
   @DisplayMetadata(
      label = "Record fetch size"
   )
   @NotMetadataImpacting
   public int fetchSize = 200;
   @Tag(8)
   @DisplayMetadata(
      label = "Encrypt connection"
   )
   @NotMetadataImpacting
   public boolean useSsl = false;
   @Tag(9)
   @DisplayMetadata(
      label = "SSL/TLS server certificate distinguished name"
   )
   @NotMetadataImpacting
   public String sslServerCertDN;
   @Tag(10)
   @DisplayMetadata(
      label = "Use timezone as connection region"
   )
   @NotMetadataImpacting
   public boolean useTimezoneAsRegion = true;
   @Tag(11)
   @DisplayMetadata(
      label = "Enable legacy dialect"
   )
   public boolean useLegacyDialect = false;
   @Tag(12)
   @DisplayMetadata(
      label = "Include synonyms"
   )
   public boolean includeSynonyms = false;
   @Tag(13)
   @DisplayMetadata(
      label = "Secret resource url"
   )
   public String secretResourceUrl;
   @Tag(14)
   @NotMetadataImpacting
   @DisplayMetadata(
      label = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)"
   )
   public boolean enableExternalQuery = false;

   public OracleConf() {
   }

   protected JdbcStoragePlugin.Config toPluginConfig(SabotContext context) {
      JdbcStoragePlugin.Config.Builder builder = JdbcStoragePlugin.Config.newBuilder().withDialect(this.getDialect()).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withAllowExternalQuery(this.supportsExternalQuery(this.enableExternalQuery)).withDatasourceFactory(() -> {
         return this.newDataSource(context.getCredentialsService());
      });
      if (!this.includeSynonyms) {
         builder.addHiddenTableType("SYNONYM");
      }

      return builder.build();
   }

   private CloseableDataSource newDataSource(CredentialsService credentialsService) throws SQLException {
      if (Strings.isNullOrEmpty(this.secretResourceUrl)) {
         Preconditions.checkNotNull(this.username, "missing username");
         Preconditions.checkNotNull(this.password, "missing password");
      }

      Preconditions.checkNotNull(this.hostname, "missing hostname");
      Preconditions.checkNotNull(this.port, "missing port");
      Preconditions.checkNotNull(this.instance, "missing instance");

      ConnectionPoolDataSource source;
      try {
         source = (ConnectionPoolDataSource)Class.forName("oracle.jdbc.pool.OracleConnectionPoolDataSource").newInstance();
      } catch (ReflectiveOperationException var4) {
         throw new RuntimeException("Cannot instantiate Oracle datasource", var4);
      }

      return this.newDataSource(source, credentialsService);
   }

   @VisibleForTesting
   CloseableDataSource newDataSource(ConnectionPoolDataSource dataSource, CredentialsService credentialsService) throws SQLException {
      Properties properties = new Properties();
      int portAsInteger = Integer.parseInt(this.port);
      properties.put("oracle.jdbc.timezoneAsRegion", Boolean.toString(this.useTimezoneAsRegion));
      properties.put("includeSynonyms", Boolean.toString(this.includeSynonyms));
      PasswordCredentials credsFromCredentialsService = null;
      if (!Strings.isNullOrEmpty(this.secretResourceUrl)) {
         try {
            URI secretURI = URI.create(this.secretResourceUrl);
            credsFromCredentialsService = (PasswordCredentials)credentialsService.getCredentials(secretURI);
         } catch (IOException var7) {
            throw new SQLException(var7.getMessage(), var7);
         }
      }

      if (!this.useSsl) {
         return newDataSource(dataSource, String.format("jdbc:oracle:thin:@//%s:%d/%s", this.hostname, portAsInteger, this.instance), this.username, credsFromCredentialsService != null ? credsFromCredentialsService.getPassword() : this.password, properties);
      } else {
         String securityOption;
         if (!Strings.isNullOrEmpty(this.sslServerCertDN)) {
            this.checkSSLServerCertDN(this.sslServerCertDN);
            securityOption = String.format("(SECURITY = (SSL_SERVER_CERT_DN = \"%s\"))", this.sslServerCertDN);
            properties.put("oracle.net.ssl_server_dn_match", "true");
         } else {
            securityOption = "";
         }

         return newDataSource(dataSource, String.format("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=%s)(PORT=%d))(CONNECT_DATA=(SERVICE_NAME=%s))%s)", this.hostname, portAsInteger, this.instance, securityOption), this.username, this.password, properties);
      }
   }

   private void checkSSLServerCertDN(String sslServerCertDN) {
      try {
         new X500Principal(sslServerCertDN);
      } catch (IllegalArgumentException var3) {
         throw new IllegalArgumentException(String.format("Server certificate DN '%s' does not respect Oracle syntax", sslServerCertDN), var3);
      }
   }

   private static CloseableDataSource newDataSource(ConnectionPoolDataSource source, String url, String username, String password, Properties properties) throws SQLException {
      try {
         MethodUtils.invokeExactMethod(source, "setURL", new Object[]{url});
         if (properties != null) {
            MethodUtils.invokeExactMethod(source, "setConnectionProperties", new Object[]{properties});
         }

         MethodUtils.invokeExactMethod(source, "setUser", new Object[]{username});
         MethodUtils.invokeExactMethod(source, "setPassword", new Object[]{password});
         return DataSources.newSharedDataSource(source);
      } catch (InvocationTargetException var7) {
         Throwable cause = var7.getCause();
         if (cause != null) {
            Throwables.throwIfInstanceOf(cause, SQLException.class);
         }

         throw new RuntimeException("Cannot instantiate Oracle datasource", var7);
      } catch (ReflectiveOperationException var8) {
         throw new RuntimeException("Cannot instantiate Oracle datasource", var8);
      }
   }

   protected LegacyDialect getLegacyDialect() {
      return OracleLegacyDialect.INSTANCE;
   }

   protected ArpDialect getArpDialect() {
      return ORACLE_ARP_DIALECT;
   }

   protected boolean getLegacyFlag() {
      return this.useLegacyDialect;
   }

   public static OracleConf newMessage() {
      OracleConf result = new OracleConf();
      result.useLegacyDialect = true;
      result.includeSynonyms = true;
      return result;
   }

   @VisibleForTesting
   public static OracleDialect getDialectSingleton() {
      return ORACLE_ARP_DIALECT;
   }
}
