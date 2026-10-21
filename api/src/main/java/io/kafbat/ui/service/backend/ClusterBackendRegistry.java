package io.kafbat.ui.service.backend;

import io.kafbat.ui.datahub.DataHubConsumerGroupService;
import io.kafbat.ui.datahub.DataHubTopicService;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.ConsumerGroupService;
import io.kafbat.ui.service.OffsetsResetService;
import io.kafbat.ui.service.TopicsService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Selects the implementation of cluster operations by cluster type.
 * Kafka clusters always use the original services.
 */
@Component
@RequiredArgsConstructor
public class ClusterBackendRegistry {

  private final TopicsService topicsService;
  private final ConsumerGroupService consumerGroupService;
  private final OffsetsResetService offsetsResetService;
  private final DataHubTopicService dataHubTopicService;
  private final DataHubConsumerGroupService dataHubConsumerGroupService;

  public TopicOperations topics(KafkaCluster cluster) {
    return cluster.isDataHub() ? dataHubTopicService : topicsService;
  }

  public ConsumerGroupOperations consumerGroups(KafkaCluster cluster) {
    return cluster.isDataHub() ? dataHubConsumerGroupService : consumerGroupService;
  }

  public OffsetsResetOperations offsetsReset(KafkaCluster cluster) {
    return cluster.isDataHub() ? dataHubConsumerGroupService : offsetsResetService;
  }

  public KafkaCluster requireKafka(KafkaCluster cluster) {
    if (cluster.isDataHub()) {
      throw new ValidationException(
          "Operation is not supported for DataHub cluster '%s'".formatted(cluster.getName()));
    }
    return cluster;
  }
}
