package io.transwarp.mapreduce;

import io.transwarp.holodesk.connector.attachment.task.ScanTaskAttachment;
import io.transwarp.holodesk.connector.attachment.task.TaskType$;
import io.transwarp.holodesk.connector.utils.shiva2.RowSetSplitHelperShiva2$;
import io.transwarp.holodesk.connector.utils.shiva2.RowSetsGroup;
import io.transwarp.holodesk.core.common.ReadMode;
import io.transwarp.holodesk.core.common.RuntimeContext;
import io.transwarp.holodesk.core.common.ScannerIteratorType;
import io.transwarp.holodesk.core.common.ScannerType;
import io.transwarp.holodesk.core.options.ReadOptions;
import io.transwarp.holodesk.sink.ArgoDBSinkTable;
import io.transwarp.holodesk.sink.partition.PartitionCalculator$;
import io.transwarp.holodesk.sink.partition.PartitionContext;
import io.transwarp.holodesk.sink.serde.ArgoDBSinkSerdeUtils;
import io.transwarp.holodesk.sink.serde.ColumnType;
import io.transwarp.mapreduce.utils.ArgodbSerdeUtils;
import io.transwarp.mapreduce.utils.MetaUtils;
import io.transwarp.mapreduce.utils.ObjectSerdeUtils;
import io.transwarp.nucleon.rdd.shiva2.CorePartition;
import io.transwarp.shiva2.ClientSpecifiedLockTypes;
import io.transwarp.shiva2.SectionLockType;
import io.transwarp.shiva2.ShivaTransactionTokenPB;
import io.transwarp.shiva2.TableLockType;
import io.transwarp.shiva2.bulk.*;
import io.transwarp.shiva2.client.ShivaClient;
import io.transwarp.shiva2.common.*;
import io.transwarp.shiva2.holo.HoloBulkReadFilterContext;
import io.transwarp.shiva2.holo.SnapshotReader;
import io.transwarp.shiva2.shiva.holo.*;
import io.transwarp.tddms.gate.TddmsEnv;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.util.TddmsGateUtils;
import org.apache.hadoop.mapreduce.*;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters$;
import scala.collection.mutable.ArrayBuffer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ArgodbMapreduceInputFormat extends InputFormat {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArgodbMapreduceInputFormat.class);

  private static final Map<String, RWTransaction> transactionMap = new ConcurrentHashMap<>();

  public static void setInput(Job job, String databaseName, String tableName) throws IOException {
    setInput(job, databaseName, tableName, "");
  }

  public static void setInput(Job job, String databaseName, String tableName, String filter) throws IOException {
    RWTransaction txn = null;
    try {
      Configuration config = job.getConfiguration();
      //config.set("io.serializations", JavaSerialization.class.getName() + "," + WritableSerialization.class.getName());
      config.set(Configs.ARGODB_TABLE_NAME, databaseName + "." + tableName);
      config.set(Configs.ARGODB_FILTER, filter);
      try {
        Class.forName(Configs.ARGODB_JDBC_DRIVER_NAME);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }

      LOGGER.info("[ARGODB] Init ArgodbMapreduceInputFormat for table [{}].", tableName);
      io.transwarp.holodesk.common.Table holoTable = MetaUtils.getTableInfo(config);

      String tddmsNS = TddmsGateUtils.getTddmsNameService(new HashMap<>());
      config.set(Configs.ARGODB_NAMESERVICE, tddmsNS);
      ShivaClient tddmsClient = TddmsEnv.getShiva2Client(tddmsNS);

      ReadOptions readOptions = getReadOptions(holoTable,
          Boolean.parseBoolean(config.get(Configs.ARGODB_FORCE_REMOTE_READ, "false")));
      config.set(Configs.ARGODB_READ_OPTIONS, ObjectSerdeUtils.serialize(readOptions));

      txn = tddmsClient.newBulkRWTransaction();
      // begin txn
      Status status = txn.begin();
      if (!status.ok()) {
        throw new Exception("Begin transaction failed.");
      }
      // serialize
      ShivaTransactionTokenPB txnTokenPB = txn.serializeToTokenPB();
      byte[] tokenBytes = txnTokenPB.toByteArray();
      config.set(Configs.ARGODB_TRANSACTION, Base64.getEncoder().encodeToString(tokenBytes));
      transactionMap.put(job.getJobName(), txn);
      LOGGER.info("[ARGODB] Job [{}] open transaction [{}].", job.getJobID(), txn.getTransactionId());
    } catch (Throwable t) {
      LOGGER.error("[ARGODB] Begin transaction failed.");
      if (txn != null) {
        try {
          txn.abort();
        } catch (Throwable at) {
          LOGGER.warn("[ARGODB] Abort transaction failed.");
        }
      }
      throw new IOException(t);
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    LOGGER.info("[ARGODB] getSplits started.");
    Configuration configuration = jobContext.getConfiguration();
    String tddmsNS = configuration.get(Configs.ARGODB_NAMESERVICE);
    String tddmsTableName = configuration.get(Configs.ARGODB_TDDMS_TABLE_NAME);
    String txnString = configuration.get(Configs.ARGODB_TRANSACTION);
    String filter = configuration.get(Configs.ARGODB_FILTER);

    List<InputSplit> splits = new ArrayList<>();
    try {
      ShivaClient tddmsClient = TddmsEnv.getShiva2Client(tddmsNS);

      ArgoDBSinkTable sinkTable = (ArgoDBSinkTable) ObjectSerdeUtils.deserialize(configuration.get(Configs.ARGODB_TABLE_METADATA));
      PartitionContext partitionContext = sinkTable.partitionContext();

      List<String> partDefineList = new ArrayList<>();
      List<String> sectionList = new ArrayList<>();
      partitionFilter(partDefineList, sectionList, filter, sinkTable);
      if (sectionList.isEmpty()) {
        return splits;
      }
      Set<ShivaPartitionName> tddmsParts = new HashSet<>();
      for (String section : sectionList) {
        tddmsParts.add(new ShivaPartitionName(section));
      }

      // use txn handler
      TransactionHandler txnHandler = getTxnHandler(tddmsClient, txnString, tddmsTableName, tddmsParts);

      HoloBulkReadFilterContext holoBulkReadFilterContext = HoloBulkReadFilterContext.newBuilder().setColumnIds(new HashSet<>()).build();
      SnapshotReader reader = tddmsClient.newHoloReaderOrDead(txnHandler, holoBulkReadFilterContext);
      Status status = reader.snapshot();
      if (!status.ok()) {
        throw new IOException("[ARGODB] Get table distribution failed.");
      }

      // TODO: use section-tablet instead
      // deal distribution
      Distribution distribution = new TransactionDistribution(reader.getDistribution());
      Map<String, Integer> sectionToSectionId = new HashMap<>();
      for (ShivaPartitionIdentifier partitionIdentifier : txnHandler.getPartitions()) {
        sectionToSectionId.put(partitionIdentifier.getPartitionName().getName(), (int) partitionIdentifier.getPartitionId().getId());
      }

      for (int i = 0; i < sectionList.size(); ++i) {
        String section = sectionList.get(i);
        ArrayBuffer<RowSet> sectionRowSets = new ArrayBuffer<>(8);
        Iterator<RowSet> sectionRowSetIter = distribution.getRowSetIterator(section);
        while (sectionRowSetIter.hasNext()) {
          sectionRowSets.$plus$eq(sectionRowSetIter.next());
        }
        RowSetsGroup[] rowSetsGroupArray = RowSetSplitHelperShiva2$.MODULE$.splitRowSetsToFiles(
            distribution, sectionRowSets.iterator(), sectionToSectionId
        );


        LOGGER.info("[ARGODB][{}] Get [{}] RowSetsGroup from section [{}].",
            tddmsTableName, rowSetsGroupArray.length, section);

        int bucketId;
        String rowSetsGroupSection;
        for (int index = 0; index < rowSetsGroupArray.length; ++index) {
          RowSetsGroup rowSetsGroup = rowSetsGroupArray[index];
          if (TaskType$.MODULE$.Scan().equals(rowSetsGroup.attachment().taskType())) {
            ScanTaskAttachment attachment = (ScanTaskAttachment) rowSetsGroup.attachment();
            bucketId = attachment.bucketId();
            rowSetsGroupSection = attachment.section();
          } else {
            bucketId = -1;
            rowSetsGroupSection = null;
          }

          CorePartition corePartition = new CorePartition(rowSetsGroup.splitContexts(), rowSetsGroup.attachment(),
              JavaConverters$.MODULE$.collectionAsScalaIterableConverter(Arrays.asList(rowSetsGroup.hosts())).asScala().toSeq(),
              rowSetsGroup.rowSetNum(), index, bucketId, rowSetsGroupSection, rowSetsGroup.rowCount(),
              rowSetsGroup.memoryUsage(), true);

          ArgodbMapreduceInputSplit argodbSplit = new ArgodbMapreduceInputSplit(corePartition);
          if (partitionContext.isPartitionTable() && !partitionContext.isRangePartition()) {
            argodbSplit.setPartDefine(partDefineList.get(i));
          }
          splits.add(argodbSplit);
        }

      }

      return splits;
    } catch (Throwable t) {
      LOGGER.error("[ARGODB] Encounter exception when get getSplits: " + t.getMessage(), t);
      throw new IOException(t);
    } finally {
      LOGGER.info("[ARGODB] getSplits completed.");
    }

  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    taskAttemptContext.getConfiguration();
    return new ArgodbMapreduceRecordReader();
  }

  private void partitionFilter(List<String> partDefineList, List<String> sectionList,
                               String filter, ArgoDBSinkTable sinkTable) throws IOException {
    try {
      PartitionContext partitionContext = sinkTable.partitionContext();
      LOGGER.info("[DEBUG] PartitionContext: " + partitionContext.sgPartTableMap());
      if (StringUtils.isBlank(filter)) {
        // no filter
        if (partitionContext.isPartitionTable()) {
          for (Map.Entry<String, String> entry : partitionContext.sgPartTableMap().entrySet()) {
            String partDefine = entry.getKey();
            String sectionName = entry.getValue();
            if (!partitionContext.isRangePartition()) { // single value partition
              partDefineList.add(partDefine);
            }
            sectionList.add(sectionName);
          }
        } else {
          sectionList.add(partitionContext.defaultPartitionName());
        }
        LOGGER.info("[ARGODB] Get all sections [{}] from table [{}].", sectionList, sinkTable.holodeskTableName());
      } else {
        // use partition filter
        if (!partitionContext.isPartitionTable()) {
          throw new IOException("[ARGODB] Filter is not supported for non-partition table.");
        }
        if (partitionContext.isRangePartition()) {
          throw new IOException("[ARGODB] Filter is not supported for range-partition table.");
        }
        if (partitionContext.partColNames().size() > 1) {
          throw new IOException("[ARGODB] Filter is not supported for multi-columns-single-value-partition table.");
        }
        int partColIdx = partitionContext.partColIndex().get(0);
        Object partWritableValue = ArgoDBSinkSerdeUtils.getWritableValue(
            ArgodbSerdeUtils.getFilterStringValue(filter), "string",
            sinkTable.columnTypes()[partColIdx], ColumnType.Java(), true, false);
        String sectionName = PartitionCalculator$.MODULE$.getPartitionNameWithPartColumnsOnly(
            Collections.singletonList(partWritableValue), partitionContext);
        LOGGER.info("[ARGODB] Get section [{}] by filter [{}] from table [{}].", sectionName, filter, sinkTable.holodeskTableName());
        if (StringUtils.isBlank(sectionName)) {
          return;
        }
        partDefineList.add(filter);
        sectionList.add(sectionName);
      }
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  private TransactionHandler getTxnHandler(ShivaClient tddmsClient, String txnString, String tddmsTableName,
                                           Set<ShivaPartitionName> sections) throws Exception {
    // deserialize
    ShivaTransactionTokenPB token = ShivaTransactionTokenPB.parseFrom(Base64.getDecoder().decode(txnString));
    ShivaTransaction shivaTransaction = new ShivaTransaction(tddmsClient, ShivaBulkTransactionToken.fromPB(token));

    // acquire locks
    ClientSpecifiedLockTypes.Builder lockBuilder = ClientSpecifiedLockTypes.newBuilder();
    lockBuilder.setTableLockType(TableLockType.kTableROShareLock);
    lockBuilder.setSectionLockType(SectionLockType.kSectionROShareLock);
    ShivaAcquireLocksOptions options = new ShivaAcquireLocksOptions();
    options.setTableIdentifier(ShivaTableIdentifier.newTableIdentifier(ShivaTableName.newName(tddmsTableName)));
    options.setLockType(ShivaTransactionLockType.READ);
    options.addPartitions(sections);
    options.setIsReadOnly(true);
    options.setNeedRenewSnapshot(true);
    options.setClientSpecifiedLockTypes(lockBuilder.build());
    ShivaContext<TransactionHandler> ctx = shivaTransaction.acquireLocks(options);
    if (!ctx.getStatus().ok()) {
      throw new IOException("[ARGODB] Reuse transaction failed: " + ctx.getStatus().getMsg(), ctx.getStatus().getCause());
    }
    return ctx.getResource();
  }

  private static ReadOptions getReadOptions(io.transwarp.holodesk.common.Table holoTable, boolean forceRemoteRead) {
    RuntimeContext runtimeContext = new RuntimeContext();
    runtimeContext.setForceRemoteRead(forceRemoteRead);
    return new ReadOptions()
        .setIsPerformanceTable(true)
        .setRuntimeContext(runtimeContext)
        .setGlobalColumnIds(holoTable.getGlobalColumnIds())
        .setNeedColumnIds(holoTable.getGlobalColumnIds())
        .setCanDoRowBlockScan(true)
        .setRpcPacketSize(1024 * 1024)
        .setNeedLoadInternalRowKey(false)
        .setPreferReadMode(ReadMode.BatchMode)
        .setScannerIteratorType(ScannerIteratorType.scan)
        .setScannerType(ScannerType.MainTable)
        .setDecryptionKeyVersionsToKeys(holoTable.getEncryptionKeyVersionsToKeys());
  }

  public static void close(Job job) {
    try {
      RWTransaction txn = transactionMap.get(job.getJobName());
      txn.abort();
    } catch (Throwable t) {
      // ignored
    }
  }

  public static HCatSchema getTableSchema(Configuration conf) throws IOException {
    try {
      return (HCatSchema) ObjectSerdeUtils.deserialize(conf.get(Configs.ARGODB_HCAT_SCHEMA));
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }
}
