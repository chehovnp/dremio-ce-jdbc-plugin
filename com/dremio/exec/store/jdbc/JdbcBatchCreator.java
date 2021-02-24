package com.dremio.exec.store.jdbc;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.CoercionReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.ProducerOperator.Creator;
import java.util.Collections;

public class JdbcBatchCreator implements Creator<JdbcSubScan> {
   public JdbcBatchCreator() {
   }

   public ProducerOperator create(FragmentExecutionContext fragmentExecContext, OperatorContext context, JdbcSubScan subScan) throws ExecutionSetupException {
      JdbcStoragePlugin plugin = (JdbcStoragePlugin)fragmentExecContext.getStoragePlugin(subScan.getPluginId());
      JdbcRecordReader innerReader = new JdbcRecordReader(context, plugin.getSource(), subScan.getSql(), plugin.getName(), subScan.getColumns(), plugin.getConfig().getFetchSize(), fragmentExecContext.cancelled(), subScan.getPluginId().getCapabilities(), plugin.getDialect().getDataTypeMapper(), subScan.getReferencedTables(), subScan.getSkippedColumns());
      CoercionReader reader = new CoercionReader(context, subScan.getColumns(), innerReader, subScan.getFullSchema());
      return new ScanOperator(subScan, context, Collections.singletonList(reader).iterator());
   }
}
