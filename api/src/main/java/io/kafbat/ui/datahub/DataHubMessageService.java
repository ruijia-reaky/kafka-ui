package io.kafbat.ui.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.SeekOutOfRangeException;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.GetRecordsResult;
import com.aliyun.datahub.client.model.PutErrorEntry;
import com.aliyun.datahub.client.model.PutRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.ShardState;
import io.kafbat.ui.emitter.DataHubRecordSource;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.CreateTopicMessageDTO;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.DeserializationService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
public class DataHubMessageService {

  // max records count DataHub returns in one getRecords call
  private static final int READ_BATCH = 1000;

  private final DataHubClientFactory clientFactory;
  private final DataHubMetadataService metadataService;
  private final DeserializationService deserializationService;

  public Mono<DataHubRecordSource> recordSource(KafkaCluster cluster, String topic) {
    return metadataService.loadTopicMetadata(cluster, topic)
        .map(snapshot -> new ShardReader(clientFactory.get(cluster), snapshot));
  }

  public Mono<RecordMetadata> sendMessage(KafkaCluster cluster, String topic, CreateTopicMessageDTO msg) {
    return metadataService.loadTopicShards(cluster, topic)
        .publishOn(Schedulers.boundedElastic())
        .map(snapshot -> send(cluster, snapshot, msg))
        .onErrorMap(DataHubErrors::translate);
  }

  private RecordMetadata send(KafkaCluster cluster, DataHubTopicSnapshot snapshot, CreateTopicMessageDTO msg) {
    @Nullable String shardId = Optional.ofNullable(msg.getPartition())
        .filter(p -> p >= 0)
        .map(String::valueOf)
        .orElse(null);
    if (shardId != null && snapshot.shards().stream()
        .noneMatch(s -> s.getShardId().equals(shardId) && s.getState() == ShardState.ACTIVE)) {
      throw new ValidationException("Invalid partition: shard %s is not active".formatted(shardId));
    }
    ProducerRecord<byte[], byte[]> record = deserializationService.producerRecordCreator(
        cluster,
        snapshot.name(),
        msg.getKeySerde().get(),
        msg.getValueSerde().get(),
        msg.getKeySerdeProperties(),
        msg.getValueSerdeProperties()
    ).create(snapshot.name(), shardId == null ? null : msg.getPartition(), msg.getKey().orElse(null),
        msg.getValue().orElse(null), msg.getHeaders());

    RecordSchema schema = snapshot.isTuple() ? snapshot.info().getRecordSchema() : null;
    RecordEntry entry = DataHubRecords.toRecordEntry(schema, record.key(), record.value(), record.headers());
    DatahubClient client = clientFactory.get(cluster);
    String project = snapshot.id().project();
    String topic = snapshot.id().topic();
    if (shardId != null) {
      client.putRecordsByShard(project, topic, shardId, List.of(entry));
    } else {
      PutRecordsResult result = client.putRecords(project, topic, List.of(entry));
      if (result.getFailedRecordCount() > 0) {
        PutErrorEntry error = result.getPutErrorEntries().getFirst();
        throw new ValidationException("DataHub rejected record: %s %s"
            .formatted(error.getErrorcode(), error.getMessage()));
      }
    }
    int partition = shardId == null ? -1 : msg.getPartition();
    return new RecordMetadata(new TopicPartition(snapshot.name(), partition), -1, 0,
        System.currentTimeMillis(), record.key() == null ? -1 : record.key().length,
        record.value() == null ? -1 : record.value().length);
  }

  private record ShardReader(DatahubClient client, DataHubTopicSnapshot snapshot) implements DataHubRecordSource {

    @Override
    public String topic() {
      return snapshot.name();
    }

    @Override
    public Map<Integer, Long> beginOffsets() {
      return snapshot.startOffsets();
    }

    @Override
    public Map<Integer, Long> endOffsets() {
      return snapshot.endOffsets();
    }

    @Override
    public List<ConsumerRecord<Bytes, Bytes>> read(int partition, long from, long to, int maxCount) {
      String shard = String.valueOf(partition);
      String cursor;
      try {
        cursor = client.getCursor(project(), dataHubTopic(), shard, CursorType.SEQUENCE, from).getCursor();
      } catch (SeekOutOfRangeException e) {
        return List.of();
      }
      RecordSchema schema = snapshot.isTuple() ? snapshot.info().getRecordSchema() : null;
      List<ConsumerRecord<Bytes, Bytes>> result = new ArrayList<>();
      while (result.size() < maxCount) {
        int batch = Math.min(READ_BATCH, maxCount - result.size());
        GetRecordsResult records = schema == null
            ? client.getRecords(project(), dataHubTopic(), shard, cursor, batch)
            : client.getRecords(project(), dataHubTopic(), shard, schema, cursor, batch);
        if (records.getRecords() == null || records.getRecords().isEmpty()) {
          break;
        }
        for (RecordEntry entry : records.getRecords()) {
          if (entry.getSequence() >= to) {
            return result;
          }
          if (entry.getSequence() >= from) {
            result.add(DataHubRecords.toConsumerRecord(snapshot.name(), partition, entry));
          }
        }
        cursor = records.getNextCursor();
      }
      return result;
    }

    @Override
    public long offsetForTimestamp(int partition, long timestamp) {
      long begin = snapshot.startOffsets().get(partition);
      long end = snapshot.endOffsets().get(partition);
      if (begin >= end) {
        return end;
      }
      try {
        return client.getCursor(project(), dataHubTopic(), String.valueOf(partition), CursorType.SYSTEM_TIME,
            timestamp).getSequence();
      } catch (SeekOutOfRangeException e) {
        return timestamp > System.currentTimeMillis() ? end : begin;
      }
    }

    private String project() {
      return snapshot.id().project();
    }

    private String dataHubTopic() {
      return snapshot.id().topic();
    }
  }
}
