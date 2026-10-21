package io.kafbat.ui.datahub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import com.aliyun.datahub.client.model.TupleRecordData;
import io.kafbat.ui.config.ClustersProperties;
import io.kafbat.ui.exception.TopicNotFoundException;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.KafkaCluster;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

class DataHubConversionsTest {

  private static final RecordSchema SCHEMA = DataHubSchemas.parse("meter_id:STRING,ts:TIMESTAMP,voltage:DOUBLE?");

  @Test
  void schemaRoundTrip() {
    assertThat(DataHubSchemas.format(SCHEMA)).isEqualTo("meter_id:STRING,ts:TIMESTAMP,voltage:DOUBLE?");
    assertThatThrownBy(() -> DataHubSchemas.parse("id")).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> DataHubSchemas.parse("id:UUID")).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> DataHubSchemas.parse(" ")).isInstanceOf(ValidationException.class);
  }

  @Test
  void tupleRecordIsConvertedToJson() {
    TupleRecordData data = new TupleRecordData(SCHEMA);
    data.setField("meter_id", "M1");
    data.setField("ts", 1_700_000_000_000_000L);
    data.setField("voltage", 220.5);
    RecordEntry entry = new RecordEntry();
    entry.setRecordData(data);
    entry.setSequence(42);
    entry.setSystemTime(1_700_000_000_000L);
    entry.addAttribute("source", "meter");

    var record = DataHubRecords.toConsumerRecord("t", 3, entry);

    assertThat(record.partition()).isEqualTo(3);
    assertThat(record.offset()).isEqualTo(42);
    assertThat(record.timestamp()).isEqualTo(1_700_000_000_000L);
    assertThat(record.key()).isNull();
    assertThat(new String(record.value().get(), StandardCharsets.UTF_8))
        .isEqualTo("{\"meter_id\":\"M1\",\"ts\":1700000000000000,\"voltage\":220.5}");
    assertThat(record.headers().lastHeader("source").value()).isEqualTo("meter".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void blobRecordIsPassedAsIs() {
    RecordEntry entry = new RecordEntry();
    entry.setRecordData(new BlobRecordData("raw".getBytes(StandardCharsets.UTF_8)));
    assertThat(DataHubRecords.toConsumerRecord("t", 0, entry).value().get())
        .isEqualTo("raw".getBytes(StandardCharsets.UTF_8));

    RecordEntry sent = DataHubRecords.toRecordEntry(null, "k".getBytes(StandardCharsets.UTF_8),
        "payload".getBytes(StandardCharsets.UTF_8),
        List.of(new RecordHeader("h", "v".getBytes(StandardCharsets.UTF_8))));
    assertThat(((BlobRecordData) sent.getRecordData()).getData()).isEqualTo("payload".getBytes(StandardCharsets.UTF_8));
    assertThat(sent.getPartitionKey()).isEqualTo("k");
    assertThat(sent.getAttributes()).containsEntry("h", "v");
  }

  @Test
  void jsonIsConvertedToTupleUsingSchemaTypes() {
    RecordEntry entry = DataHubRecords.toRecordEntry(SCHEMA, null,
        "{\"meter_id\":\"M2\",\"ts\":\"1700000000000000\"}".getBytes(StandardCharsets.UTF_8), List.of());
    TupleRecordData data = (TupleRecordData) entry.getRecordData();
    assertThat(data.getField("meter_id")).isEqualTo("M2");
    assertThat(data.getField("ts")).isEqualTo(1_700_000_000_000_000L);
    assertThat(data.getField("voltage")).isNull();
  }

  @Test
  void invalidTupleJsonIsRejected() {
    assertThatThrownBy(() -> DataHubRecords.toRecordEntry(SCHEMA, null, bytes("not json"), List.of()))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> DataHubRecords.toRecordEntry(SCHEMA, null, bytes("[1]"), List.of()))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> DataHubRecords.toRecordEntry(SCHEMA, null, bytes("{\"ts\":1}"), List.of()))
        .hasMessageContaining("meter_id");
    assertThatThrownBy(() -> DataHubRecords.toRecordEntry(SCHEMA, null,
        bytes("{\"meter_id\":\"M\",\"ts\":1,\"x\":1}"), List.of()))
        .hasMessageContaining("Unknown field 'x'");
    assertThatThrownBy(() -> DataHubRecords.toRecordEntry(SCHEMA, null,
        bytes("{\"meter_id\":\"M\",\"ts\":\"abc\"}"), List.of()))
        .hasMessageContaining("TIMESTAMP");
  }

  @Test
  void subscriptionOffsetsAreClampedAndShifted() {
    var snapshot = new DataHubTopicSnapshot("t", new DataHubTopicName("p", "t"), null, List.of(),
        Map.of(0, 5L, 1, 0L), Map.of(0, 10L, 1, 0L), List.of());

    Map<String, SubscriptionOffset> offsets =
        DataHubConsumerGroupService.toSubscriptionOffsets(snapshot, Map.of(0, 7L, 1, 3L));
    assertThat(offsets.get("0").getSequence()).isEqualTo(6L);
    assertThat(offsets.get("1").getSequence()).isEqualTo(-1L);

    assertThat(DataHubConsumerGroupService.toSubscriptionOffsets(snapshot, Map.of(0, 100L)).get("0").getSequence())
        .isEqualTo(9L);
    assertThat(DataHubConsumerGroupService.toSubscriptionOffsets(snapshot, Map.of(0, 0L)).get("0").getSequence())
        .isEqualTo(4L);
    assertThatThrownBy(() -> DataHubConsumerGroupService.toSubscriptionOffsets(snapshot, Map.of(7, 0L)))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void uncommittedShardsStartFromOldestRecord() {
    SubscriptionOffset consumed = new SubscriptionOffset();
    consumed.setSequence(6L);
    SubscriptionOffset none = new SubscriptionOffset();
    none.setSequence(-1L);
    SubscriptionOffset expired = new SubscriptionOffset();
    expired.setSequence(2L);

    var committed = DataHubMetadataService.toCommittedOffsets("t",
        Map.of("0", consumed, "1", none, "3", expired), Map.of(0, 0L, 1, 4L, 2, 0L, 3, 5L));

    assertThat(committed).containsOnly(
        Map.entry(new TopicPartition("t", 0), 7L),
        Map.entry(new TopicPartition("t", 1), 4L),
        Map.entry(new TopicPartition("t", 2), 0L),
        Map.entry(new TopicPartition("t", 3), 5L));
  }

  @Test
  void lifecycleDaysFromRetention() {
    assertThat(DataHubTopicService.lifecycleDays(null)).isEqualTo(1);
    assertThat(DataHubTopicService.lifecycleDays("1")).isEqualTo(1);
    assertThat(DataHubTopicService.lifecycleDays("86400000")).isEqualTo(1);
    assertThat(DataHubTopicService.lifecycleDays("86400001")).isEqualTo(2);
    assertThat(DataHubTopicService.lifecycleDays("604800000")).isEqualTo(7);
    assertThatThrownBy(() -> DataHubTopicService.lifecycleDays("-1")).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> DataHubTopicService.lifecycleDays("abc")).isInstanceOf(ValidationException.class);
  }

  @Test
  void topicNamesArePrefixedOnlyWithSeveralProjects() {
    KafkaCluster single = cluster(List.of("p1"));
    assertThat(DataHubTopicName.parse(single, "orders")).isEqualTo(new DataHubTopicName("p1", "orders"));
    assertThat(DataHubTopicName.format(single, "p1", "orders")).isEqualTo("orders");

    KafkaCluster multi = cluster(List.of("p1", "p2"));
    assertThat(DataHubTopicName.parse(multi, "p2.orders")).isEqualTo(new DataHubTopicName("p2", "orders"));
    assertThat(DataHubTopicName.format(multi, "p2", "orders")).isEqualTo("p2.orders");
    assertThatThrownBy(() -> DataHubTopicName.parse(multi, "orders")).isInstanceOf(TopicNotFoundException.class);
    assertThatThrownBy(() -> DataHubTopicName.parse(multi, "p3.orders")).isInstanceOf(TopicNotFoundException.class);
  }

  private static KafkaCluster cluster(List<String> projects) {
    var config = new ClustersProperties.DataHubConfig();
    config.setEndpoint("https://dh.example.com");
    config.setProjects(projects);
    return KafkaCluster.builder()
        .name("dh")
        .type(ClustersProperties.ClusterType.DATAHUB)
        .datahub(config)
        .build();
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}
