package io.kafbat.ui.service.backend;

import io.kafbat.ui.model.KafkaCluster;
import java.util.Collection;
import java.util.Map;
import reactor.core.publisher.Mono;

public interface OffsetsResetOperations {

  Mono<Void> resetToEarliest(KafkaCluster cluster, String group, String topic, Collection<Integer> partitions);

  Mono<Void> resetToLatest(KafkaCluster cluster, String group, String topic, Collection<Integer> partitions);

  Mono<Void> resetToTimestamp(KafkaCluster cluster, String group, String topic, Collection<Integer> partitions,
                              long targetTimestamp);

  Mono<Void> resetToOffsets(KafkaCluster cluster, String group, String topic, Map<Integer, Long> targetOffsets);
}
