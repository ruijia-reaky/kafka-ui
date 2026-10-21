package io.kafbat.ui.emitter;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;

/**
 * Read access to one DataHub topic, already converted to Kafka records (shard = partition, sequence = offset).
 */
public interface DataHubRecordSource {

  String topic();

  /**
   * Start offsets (oldest available sequence) of readable shards.
   */
  Map<Integer, Long> beginOffsets();

  /**
   * End offsets (sequence of the next record to be written) of readable shards.
   */
  Map<Integer, Long> endOffsets();

  /**
   * Returns up to {@code maxCount} records of the shard with offsets in [{@code from}, {@code to}).
   */
  List<ConsumerRecord<Bytes, Bytes>> read(int partition, long from, long to, int maxCount);

  /**
   * Offset of the first record with timestamp >= {@code timestamp}, or end offset if there is none.
   */
  long offsetForTimestamp(int partition, long timestamp);
}
