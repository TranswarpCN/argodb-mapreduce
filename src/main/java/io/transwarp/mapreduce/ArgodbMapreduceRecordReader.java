package io.transwarp.mapreduce;

import io.transwarp.holodesk.connector.attachment.task.ScanTaskAttachment;
import io.transwarp.holodesk.connector.scan.shiva2.ScanClientShiva2;
import io.transwarp.holodesk.connector.utils.shiva2.RowSetUtilsShiva2;
import io.transwarp.holodesk.core.iterator.ConvertRowReferenceIteratorToRowResultIterator;
import io.transwarp.holodesk.core.iterator.RowReferenceIterator;
import io.transwarp.holodesk.core.options.ReadOptions;
import io.transwarp.holodesk.core.result.RowResult;
import io.transwarp.holodesk.sink.ArgoDBSinkClient;
import io.transwarp.holodesk.sink.ArgoDBSinkTable;
import io.transwarp.holodesk.sink.partition.PartitionContext;
import io.transwarp.inceptor.memstore2.ColumnarStructObjectInspector;
import io.transwarp.inceptor.memstore2.HolodeskCoreInceptorFastDeserializer;
import io.transwarp.mapreduce.utils.ArgodbSerdeUtils;
import io.transwarp.mapreduce.utils.ObjectSerdeUtils;
import io.transwarp.nucleon.rdd.shiva2.CorePartition;
import io.transwarp.shiva2.shiva.client.ShivaClient;
import io.transwarp.shiva2.shiva.holo.RowSet;
import io.transwarp.tddms.gate.TddmsEnv;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.util.ShivaEnv;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ArgodbMapreduceRecordReader extends RecordReader<WritableComparable<?>, Writable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArgodbMapreduceRecordReader.class);

  private String tddmsNS;
  ShivaClient tddmsClient;
  private String tddmsTableName;
  private ArgoDBSinkTable sinkTable;
  private Configuration hiveConf;


  List<CorePartition> corePartitions;
  private int corePartitionIdx = 0;
  Map<String, String> partValueMap; // LinkedHashMap

  private ReadOptions readOptions = null;
  private Iterator<RowResult> rowResultIterator = null;
  private RowReferenceIterator rowReferenceIterator = null;
  private RowSet[] rowSets = null;
  private HolodeskCoreInceptorFastDeserializer deserializer = null;
  private boolean needNewIter = true;

  private HCatSchema hCatSchema = null;
  private HCatRecord currentValue = null;

  private List<String> columnNames;
  private List<String> columnTypes;


  private int rowCount = 0;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (!(inputSplit instanceof ArgodbMapreduceInputSplit)) {
      throw new IOException("[ARGODB] InputSplit is not ArgodbMapreduceInputSplit.");
    }

    Configuration configuration = taskAttemptContext.getConfiguration();
    tddmsNS = configuration.get(Configs.ARGODB_NAMESERVICE);
    tddmsTableName = configuration.get(Configs.ARGODB_TDDMS_TABLE_NAME);

    try {
      Map<String, String> hiveConfMap =
          (Map<String, String>) ObjectSerdeUtils.deserialize(configuration.get(Configs.ARGODB_HIVE_CONF_MAP));
      hiveConf = new Configuration();
      for (Map.Entry<String, String> item : hiveConfMap.entrySet()) {
        hiveConf.set(item.getKey(), item.getValue());
      }

      TddmsEnv.init(hiveConf);
      ShivaEnv.setHiveConf(hiveConf);

      sinkTable = (ArgoDBSinkTable) ObjectSerdeUtils.deserialize(configuration.get(Configs.ARGODB_TABLE_METADATA));
      readOptions = (ReadOptions) ObjectSerdeUtils.deserialize(configuration.get(Configs.ARGODB_READ_OPTIONS));
      tddmsClient = TddmsEnv.getHoloShiva2Client(tddmsNS);

      hCatSchema = (HCatSchema) ObjectSerdeUtils.deserialize(configuration.get(Configs.ARGODB_HCAT_SCHEMA));
      io.transwarp.holodesk.common.Table holoTable = sinkTable.holodeskTable();

      columnNames = (List<String>) ObjectSerdeUtils.deserialize(configuration.get(Configs.ARGODB_COLUMN_NAMES));
      columnTypes = (List<String>) ObjectSerdeUtils.deserialize(configuration.get(Configs.ARGODB_COLUMN_TYPES));

      deserializer =
          ArgoDBSinkClient.generateHolodeskCoreInceptorFastDeserializer(columnNames, columnTypes,holoTable.colNum());

      ArgodbMapreduceInputSplit argodbSplit = (ArgodbMapreduceInputSplit) inputSplit;
      corePartitions = argodbSplit.getCorePartitions();
      if (isSingleValuePartition(sinkTable)) {
        String partDefine = argodbSplit.getPartDefine();
        partValueMap = getPartValueMapFromPartDefine(partDefine);
      }

    } catch (Throwable t) {
      LOGGER.error("[ARGODB] Encounter exception when initialize ArgodbMapreduceRecordReader: " + t.getMessage(), t);
      throw new IOException(t);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (needNewIter) {
      if (corePartitionIdx < corePartitions.size()) {
        newIter(corePartitions.get(corePartitionIdx));
        corePartitionIdx += 1;
        needNewIter = false;
        corePartitionIdx += 1;
      } else {
        return false; // no more iterators
      }
    }
    if (!this.rowResultIterator.hasNext()) {
      needNewIter = true;
      return nextKeyValue();
    }
    return true;
  }

  private void newIter(CorePartition corePartition) throws IOException {
    this.rowSets = RowSetUtilsShiva2.getRowSetsFromSplitContexts(tddmsClient, corePartition.splitContexts(), new HashSet<>());
    if (this.rowSets.length != corePartition.rowSetNum()) {
      throw new IOException("[ARGODB][" + tddmsTableName + "] " +
          "RowSet number has changed before scan, current RowSets: [" +
          this.rowSets.length + "], expected RowSets: [" + corePartition.rowSetNum() + "].");
    }
    String tableId = corePartition.splitContexts()[0].getTableId();
    if (corePartition.attachment() instanceof ScanTaskAttachment) {
      this.rowReferenceIterator = ScanClientShiva2.scanPerformanceTable(
          tddmsClient, tableId, readOptions, this.rowSets, true,
          ((ScanTaskAttachment) corePartition.attachment()).sectionId()
      );
      this.rowResultIterator = ConvertRowReferenceIteratorToRowResultIterator.newRowResultIterator(this.rowReferenceIterator);
    } else {
      throw new IOException("[ARGODB][" + tddmsTableName + "] Only support ScanTaskAttachment for performance table !");
    }
  }


  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public HCatRecord getCurrentValue() throws IOException, InterruptedException {
    ++rowCount;
    return analyzeRow(rowResultIterator.next());
  }


  private Map<String, String> getPartValueMapFromPartDefine(String partDefine) {
    Map<String, String> partValue = new LinkedHashMap<>();
    String[] partDefineArr = partDefine.split("/");
    for (String singlePartDefine : partDefineArr) {
      String column = singlePartDefine.split("=")[0];
      partValue.put(column, ArgodbSerdeUtils.getFilterStringValue(singlePartDefine));
    }
    return partValue;
  }

  private HCatRecord analyzeRow(RowResult rowResult) throws IOException {
    List<Object> result = new ArrayList<>(Arrays.asList(deserializer.deserialize(rowResult, 0)));
    // single value partition column(s)
    if (isSingleValuePartition(sinkTable)) {
      for (Map.Entry<String, String> partValue : partValueMap.entrySet()) {
        String column = partValue.getKey();
        String value = partValue.getValue();
        if (hiveConf.get(Configs.DEFAULT_PARTITION_NAME, "__HIVE_DEFAULT_PARTITION__").equals(value)) {
          result.add(null);
        } else if (hiveConf.get(Configs.DEFAULT_PARTITION_NAME_EMPTYSTR, "__HIVE_DEFAULT_PARTITION_EMPTYSTR__").equals(value)) {
          result.add("");
        } else {
          result.add(ArgodbSerdeUtils.convertStringValueToObject(column, value, hCatSchema));
        }
      }

    }
    currentValue = new DefaultHCatRecord(result);
    return currentValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("[ARGODB] Read [{}] rows totally.", rowCount);
    if (this.rowReferenceIterator != null) {
      this.rowReferenceIterator.close();
    }
  }

  private boolean isSingleValuePartition(ArgoDBSinkTable sinkTable) {
    PartitionContext partitionContext = sinkTable.partitionContext();
    return partitionContext.isPartitionTable() && !partitionContext.isRangePartition();
  }

}
