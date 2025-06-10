package io.transwarp.mapreduce.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;

public class ArgodbSerdeUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArgodbSerdeUtils.class);

  public static String getFilterStringValue(String filter) {
    int start = filter.indexOf("=");
    String value = filter.substring(start + 1);
    if (value.startsWith("\"") && value.endsWith("\"")) {
      value = value.substring(1, value.length() - 1);
    }
    LOGGER.info("[ARGODB] Get filter string value: [{}].", value);
    return value;
  }

  public static String getFilterColumn(String filter) {
    if (StringUtils.isBlank(filter)) {
      return "";
    }
    return filter.substring(0, filter.indexOf("="));
  }

  public static Object convertStringValueToObject(String columnName, String value, HCatSchema schema) throws IOException {
    try {
      HCatFieldSchema.Type hCatType = schema.get(columnName).getType();
      switch (hCatType) {
        case BOOLEAN:
          return Boolean.parseBoolean(value);
        case TINYINT:
          return value.getBytes();
        case SMALLINT:
          return Short.parseShort(value);
        case INT:
          return Integer.parseInt(value);
        case BIGINT:
          return Long.parseLong(value);
        case FLOAT:
          return Float.parseFloat(value);
        case DOUBLE:
          return Double.parseDouble(value);
        case STRING:
        case CHAR:
        case VARCHAR:
          return String.valueOf(value);
        case DATE:
          return Date.valueOf(value);
        case TIMESTAMP:
          return Timestamp.valueOf(value);
        default:
          return value;
      }
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }


  // based on org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type
  public static HCatFieldSchema.Type primitiveTypeInfoToHCatFieldSchemaType(PrimitiveTypeInfo primitiveTypeInfo) throws IOException {
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case BOOLEAN:
        return HCatFieldSchema.Type.valueOf("BOOLEAN");
      case BYTE:
        return HCatFieldSchema.Type.valueOf("TINYINT");
      case SHORT:
        return HCatFieldSchema.Type.valueOf("SMALLINT");
      case INT:
        return HCatFieldSchema.Type.valueOf("INT");
      case LONG:
        return HCatFieldSchema.Type.valueOf("BIGINT");
      case FLOAT:
        return HCatFieldSchema.Type.valueOf("FLOAT");
      case DECIMAL:
        return HCatFieldSchema.Type.valueOf("DECIMAL");
      case STRING:
        return HCatFieldSchema.Type.valueOf("STRING");
      case CHAR:
        return HCatFieldSchema.Type.valueOf("CHAR");
      case VARCHAR:
        return HCatFieldSchema.Type.valueOf("VARCHAR");
      case BINARY:
        return HCatFieldSchema.Type.valueOf("BINARY");
      case DATE:
        return HCatFieldSchema.Type.valueOf("DATE");
      case TIMESTAMP:
        return HCatFieldSchema.Type.valueOf("TIMESTAMP");
      default:
        throw new IOException("[ARGODB] Unsupported data type: " + primitiveTypeInfo.getPrimitiveCategory().name());
    }
  }

}
