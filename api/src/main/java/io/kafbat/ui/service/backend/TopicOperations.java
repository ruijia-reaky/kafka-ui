package io.kafbat.ui.service.backend;

import io.kafbat.ui.model.InternalTopic;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.model.TopicCreationDTO;
import java.util.List;
import org.apache.kafka.clients.admin.ConfigEntry;
import reactor.core.publisher.Mono;

public interface TopicOperations {

  Mono<List<InternalTopic>> getTopics(KafkaCluster cluster, String search, Boolean showInternal, Boolean fts);

  Mono<List<InternalTopic>> loadTopics(KafkaCluster cluster, List<String> topics);

  Mono<InternalTopic> getTopicDetails(KafkaCluster cluster, String topicName);

  Mono<List<ConfigEntry>> getTopicConfigs(KafkaCluster cluster, String topicName);

  Mono<InternalTopic> createTopic(KafkaCluster cluster, TopicCreationDTO topicCreation);

  Mono<Void> deleteTopic(KafkaCluster cluster, String topicName);
}
