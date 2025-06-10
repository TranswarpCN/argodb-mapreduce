package io.transwarp.mapreduce.utils;

import io.transwarp.holodesk.sink.ArgoDBConfig;
import io.transwarp.holodesk.sink.ArgoDBSinkClient;
import io.transwarp.holodesk.sink.ArgoDBSinkConfig;
import io.transwarp.holodesk.sink.ArgoDBSinkTable;
import io.transwarp.holodesk.sink.meta.SingleTable;
import io.transwarp.mapreduce.Configs;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class MetaUtils {

  public static final Logger LOGGER = LoggerFactory.getLogger(MetaUtils.class);

  public static io.transwarp.holodesk.common.Table getTableInfo(Configuration conf) throws IOException {
    String url = conf.get(Configs.ARGODB_JDBC_URL);
    String user = conf.get(Configs.ARGODB_JDBC_USER);
    String passwd = conf.get(Configs.ARGODB_JDBC_PASSWORD);
    String tableName = conf.get(Configs.ARGODB_TABLE_NAME);

    LOGGER.info("[ARGODB] Connection url: [{}]", url);
    ArgoDBSinkClient client = null;
    try {
      ArgoDBConfig argoDBConfig = ArgoDBConfig.builder().url(url).user(user).passwd(passwd).build();
      ArgoDBSinkConfig argoDBSinkConfig = ArgoDBSinkConfig.builder()
          .argoConfig(argoDBConfig)
          .useAutoFileClean(false)
          .isReadMode(true)
          .build();

      client = new ArgoDBSinkClient(argoDBSinkConfig);
      client.init();
      client.openTable(tableName);
      conf.set(Configs.ARGODB_HIVE_CONF_MAP, ObjectSerdeUtils.serialize(ArgoDBSinkClient.getHiveConfMap()));

      SingleTable singleTable = client.getTable(tableName);
      ArgoDBSinkTable sinkTable = singleTable.getSinkTable();

      String filterColumn = ArgodbSerdeUtils.getFilterColumn(conf.get(Configs.ARGODB_FILTER));
      if (StringUtils.isNotBlank(filterColumn) && !singleTable.getColumnNames().contains(filterColumn)) {
        throw new IOException(String.format("[ARGODB] Filter column [%s] is not in table [%s].", filterColumn, tableName));
      }

      conf.set(Configs.ARGODB_TDDMS_TABLE_NAME, sinkTable.holodeskTableName());
      conf.set(Configs.ARGODB_TABLE_METADATA, ObjectSerdeUtils.serialize(sinkTable));

      List<HCatFieldSchema> fieldSchemas = new ArrayList<>();
      for (int i = 0; i < singleTable.getColumnNames().size(); ++i) {
        String name = singleTable.getColumnNames().get(i);
        PrimitiveTypeInfo typeInfo = TypeInfoFactory.getPrimitiveTypeInfo(singleTable.getColumnTypes().get(i));
        HCatFieldSchema.Type hCatType = ArgodbSerdeUtils.primitiveTypeInfoToHCatFieldSchemaType(typeInfo);
        HCatFieldSchema fieldSchema = new HCatFieldSchema(name, hCatType, "");
        fieldSchemas.add(fieldSchema);
      }
      HCatSchema schema = new HCatSchema(fieldSchemas);
      conf.set(Configs.ARGODB_HCAT_SCHEMA, ObjectSerdeUtils.serialize(schema));
      conf.set(Configs.ARGODB_COLUMN_NAMES, ObjectSerdeUtils.serialize(singleTable.getColumnNames()));
      conf.set(Configs.ARGODB_COLUMN_TYPES, ObjectSerdeUtils.serialize(singleTable.getColumnTypes()));

      return sinkTable.holodeskTable();
    } catch (Exception e) {
      LOGGER.error("Get argodb table info failed: " + e.getMessage(), e);
      throw new IOException(e);
    } finally {
      try {
        if (client != null) {
          client.close();
        }
      } catch (Throwable t) {
        // ignored
      }
    }
  }
}
