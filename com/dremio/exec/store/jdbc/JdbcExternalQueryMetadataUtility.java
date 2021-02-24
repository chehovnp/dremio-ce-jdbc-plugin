package com.dremio.exec.store.jdbc;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.jdbc.dialect.JdbcToFieldMapping;
import com.dremio.exec.store.jdbc.dialect.TypeMapper;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.dremio.exec.tablefunctions.DremioCalciteResource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcExternalQueryMetadataUtility {
   private static final Logger LOGGER = LoggerFactory.getLogger(JdbcExternalQueryMetadataUtility.class);
   private static final long METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS;

   private JdbcExternalQueryMetadataUtility() {
   }

   public static BatchSchema getBatchSchema(DataSource source, JdbcDremioSqlDialect dialect, String query, String sourceName) {
      ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory(Thread.currentThread().getName() + ":jdbc-eq-metadata"));
      Callable<BatchSchema> retrieveMetadata = () -> {
         return getExternalQueryMetadataFromSource(source, dialect, query, sourceName);
      };
      Future<BatchSchema> future = executor.submit(retrieveMetadata);
      return handleMetadataFuture(future, executor, METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS);
   }

   static BatchSchema handleMetadataFuture(Future<BatchSchema> future, ExecutorService executor, long timeout) {
      BatchSchema var4;
      try {
         var4 = (BatchSchema)future.get(timeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException var11) {
         LOGGER.debug("Timeout while fetching metadata", var11);
         throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError(var11));
      } catch (InterruptedException var12) {
         Thread.currentThread().interrupt();
         throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError(var12));
      } catch (ExecutionException var13) {
         Throwable cause = var13.getCause();
         if (cause instanceof CalciteContextException) {
            throw (CalciteContextException)cause;
         }

         throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError(cause));
      } finally {
         future.cancel(true);
         executor.shutdownNow();
      }

      return var4;
   }

   private static BatchSchema getExternalQueryMetadataFromSource(DataSource source, JdbcDremioSqlDialect dialect, String query, String sourceName) throws SQLException {
      Connection conn = source.getConnection();
      Throwable var5 = null;

      BatchSchema var10;
      try {
         PreparedStatement stmt = conn.prepareStatement(query);
         Throwable var7 = null;

         try {
            stmt.setQueryTimeout(Ints.saturatedCast(TimeUnit.MILLISECONDS.toSeconds(METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS)));
            ResultSetMetaData metaData = stmt.getMetaData();
            if (metaData == null) {
               throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryInvalidError(sourceName));
            }

            List<JdbcToFieldMapping> mappings = dialect.getDataTypeMapper().mapJdbcToArrowFields((TypeMapper.UnrecognizedTypeMarker)null, (TypeMapper.AddPropertyCallback)null, (TypeMapper.InvalidMetaDataCallback)((message) -> {
               throw UserException.invalidMetadataError().addContext(message).buildSilently();
            }), (Connection)conn, (ResultSetMetaData)metaData, (Set)null, true);
            var10 = BatchSchema.newBuilder().addFields((Iterable)mappings.stream().map(JdbcToFieldMapping::getField).collect(Collectors.toList())).build();
         } catch (Throwable var21) {
            var7 = var21;
            throw var21;
         } finally {
            if (stmt != null) {
               $closeResource(var7, stmt);
            }

         }
      } catch (Throwable var23) {
         var5 = var23;
         throw var23;
      } finally {
         if (conn != null) {
            $closeResource(var5, conn);
         }

      }

      return var10;
   }

   @VisibleForTesting
   static CalciteContextException newValidationError(ExInst<SqlValidatorException> exceptionExInst) {
      return SqlUtil.newContextException(SqlParserPos.ZERO, exceptionExInst);
   }

   static {
      METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS = TimeUnit.SECONDS.toMillis(120L);
   }
}
