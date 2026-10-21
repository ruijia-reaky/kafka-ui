package io.kafbat.ui.datahub;

import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kafbat.ui.exception.ValidationException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Bytes;

/**
 * Conversion between DataHub records and Kafka records. Blob data is passed as is, Tuple records
 * are represented as JSON objects (field name -> value), attributes are mapped to headers.
 */
public final class DataHubRecords {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private DataHubRecords() {
  }

  public static ConsumerRecord<Bytes, Bytes> toConsumerRecord(String topic, int partition, RecordEntry entry) {
    byte[] value = switch (entry.getRecordData()) {
      case BlobRecordData blob -> blob.getData();
      case TupleRecordData tuple -> tupleToJson(tuple);
      case null -> null;
      default -> throw new IllegalStateException("Unsupported record data " + entry.getRecordData().getClass());
    };
    RecordHeaders headers = new RecordHeaders();
    Optional.ofNullable(entry.getAttributes()).orElse(Map.of())
        .forEach((k, v) -> headers.add(new RecordHeader(k, v == null ? null : v.getBytes(StandardCharsets.UTF_8))));
    return new ConsumerRecord<>(
        topic,
        partition,
        entry.getSequence(),
        entry.getSystemTime(),
        TimestampType.LOG_APPEND_TIME,
        -1,
        value == null ? -1 : value.length,
        null,
        value == null ? null : new Bytes(value),
        headers,
        Optional.empty()
    );
  }

  /**
   * Builds a DataHub record from serialized Kafka message parts. For Tuple topics value must be a JSON object.
   */
  public static RecordEntry toRecordEntry(@Nullable RecordSchema schema,
                                          @Nullable byte[] key,
                                          @Nullable byte[] value,
                                          Iterable<? extends Header> headers) {
    RecordEntry entry = new RecordEntry();
    if (schema == null) {
      entry.setRecordData(new BlobRecordData(value == null ? new byte[0] : value));
    } else {
      entry.setRecordData(jsonToTuple(schema, value));
    }
    if (key != null) {
      entry.setPartitionKey(new String(key, StandardCharsets.UTF_8));
    }
    for (Header h : headers) {
      entry.addAttribute(h.key(), h.value() == null ? "" : new String(h.value(), StandardCharsets.UTF_8));
    }
    return entry;
  }

  static byte[] tupleToJson(TupleRecordData tuple) {
    Map<String, Object> fields = new LinkedHashMap<>();
    for (Field f : tuple.getRecordSchema().getFields()) {
      fields.put(f.getName(), tuple.getField(f.getName()));
    }
    try {
      return MAPPER.writeValueAsBytes(fields);
    } catch (Exception e) {
      throw new IllegalStateException("Can't serialize tuple record", e);
    }
  }

  static TupleRecordData jsonToTuple(RecordSchema schema, @Nullable byte[] value) {
    JsonNode json;
    try {
      json = value == null ? null : MAPPER.readTree(value);
    } catch (Exception e) {
      throw new ValidationException("Tuple topic value should be a JSON object: " + e.getMessage());
    }
    if (!(json instanceof ObjectNode obj)) {
      throw new ValidationException("Tuple topic value should be a JSON object with fields: "
          + DataHubSchemas.format(schema));
    }
    TupleRecordData data = new TupleRecordData(schema);
    obj.fieldNames().forEachRemaining(name -> {
      if (!schema.containsField(name)) {
        throw new ValidationException("Unknown field '%s', schema: %s".formatted(name, DataHubSchemas.format(schema)));
      }
    });
    for (Field f : schema.getFields()) {
      JsonNode node = obj.get(f.getName());
      if (node == null || node.isNull()) {
        if (!f.isAllowNull()) {
          throw new ValidationException("Field '%s' can't be null".formatted(f.getName()));
        }
        continue;
      }
      data.setField(f.getName(), convert(f, node));
    }
    return data;
  }

  private static Object convert(Field field, JsonNode node) {
    try {
      return switch (field.getType()) {
        case BIGINT, TIMESTAMP -> node.isNumber() ? node.longValue() : Long.parseLong(node.asText());
        case INTEGER -> node.isNumber() ? node.intValue() : Integer.parseInt(node.asText());
        case SMALLINT -> node.isNumber() ? node.shortValue() : Short.parseShort(node.asText());
        case TINYINT -> node.isNumber() ? (byte) node.intValue() : Byte.parseByte(node.asText());
        case DOUBLE -> node.isNumber() ? node.doubleValue() : Double.parseDouble(node.asText());
        case FLOAT -> node.isNumber() ? node.floatValue() : Float.parseFloat(node.asText());
        case DECIMAL -> node.isNumber() ? node.decimalValue() : new BigDecimal(node.asText());
        case BOOLEAN -> node.isBoolean() ? node.booleanValue() : Boolean.parseBoolean(node.asText());
        case JSON -> node.isTextual() ? node.asText() : node.toString();
        default -> node.isTextual() ? node.asText() : node.toString();
      };
    } catch (NumberFormatException e) {
      throw new ValidationException(
          "Field '%s' should be %s, got: %s".formatted(field.getName(), field.getType(), node));
    }
  }
}
