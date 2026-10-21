package io.kafbat.ui.datahub;

import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordSchema;
import io.kafbat.ui.exception.ValidationException;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Compact text form of a Tuple topic schema: {@code name:TYPE[,name:TYPE...]}, e.g.
 * {@code meter_id:STRING,ts:TIMESTAMP,voltage:DOUBLE}. A {@code ?} suffix on the type marks a nullable field.
 */
public final class DataHubSchemas {

  private DataHubSchemas() {
  }

  public static String format(RecordSchema schema) {
    if (schema == null) {
      return "";
    }
    return schema.getFields().stream()
        .map(f -> f.getName() + ":" + f.getType().name() + (f.isAllowNull() ? "?" : ""))
        .collect(Collectors.joining(","));
  }

  public static RecordSchema parse(String text) {
    if (text == null || text.isBlank()) {
      throw new ValidationException("datahub.schema is required for TUPLE topics, e.g. id:STRING,value:DOUBLE");
    }
    RecordSchema schema = new RecordSchema();
    for (String part : text.split(",")) {
      String[] nameAndType = part.trim().split(":");
      if (nameAndType.length != 2 || nameAndType[0].isBlank()) {
        throw new ValidationException("Invalid schema field definition '%s', expected name:TYPE".formatted(part));
      }
      String type = nameAndType[1].trim().toUpperCase(Locale.ROOT);
      boolean nullable = type.endsWith("?");
      if (nullable) {
        type = type.substring(0, type.length() - 1);
      }
      try {
        schema.addField(new Field(nameAndType[0].trim(), FieldType.valueOf(type), nullable));
      } catch (IllegalArgumentException e) {
        throw new ValidationException("Unknown DataHub field type '%s'".formatted(type));
      }
    }
    return schema;
  }
}
