package io.kafbat.ui.datahub;

import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.RecordType;
import io.kafbat.ui.config.ClustersProperties;
import io.kafbat.ui.exception.TopicNotFoundException;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.InternalTopic;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.model.Statistics;
import io.kafbat.ui.model.TopicCreationDTO;
import io.kafbat.ui.service.StatisticsCache;
import io.kafbat.ui.service.backend.TopicOperations;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
public class DataHubTopicService implements TopicOperations {

  static final int DEFAULT_LIFECYCLE_DAYS = 1;

  private final DataHubClientFactory clientFactory;
  private final DataHubMetadataService metadataService;
  private final StatisticsCache statisticsCache;
  private final ClustersProperties clustersProperties;

  @Override
  public Mono<List<InternalTopic>> getTopics(KafkaCluster cluster, String search, Boolean showInternal, Boolean fts) {
    Statistics stats = statisticsCache.get(cluster);
    boolean useFts = clustersProperties.getFts().use(fts);
    return Mono.fromCallable(() ->
        List.copyOf(stats.getClusterState().getTopicIndex().find(search, showInternal, useFts, null)));
  }

  @Override
  public Mono<List<InternalTopic>> loadTopics(KafkaCluster cluster, List<String> topics) {
    var topicStates = statisticsCache.get(cluster).getClusterState().getTopicStates();
    return Mono.just(topics.stream()
        .map(topicStates::get)
        .filter(Objects::nonNull)
        .map(s -> InternalTopic.from(s, clustersProperties.getInternalTopicPrefix()))
        .toList());
  }

  @Override
  public Mono<InternalTopic> getTopicDetails(KafkaCluster cluster, String topicName) {
    return metadataService.refreshTopic(cluster, topicName, statisticsCache)
        .then(Mono.defer(() -> loadTopics(cluster, List.of(topicName))))
        .flatMap(lst -> lst.isEmpty() ? Mono.error(new TopicNotFoundException()) : Mono.just(lst.getFirst()));
  }

  @Override
  public Mono<List<ConfigEntry>> getTopicConfigs(KafkaCluster cluster, String topicName) {
    return metadataService.loadTopic(cluster, topicName).map(DataHubTopicSnapshot::configs);
  }

  @Override
  public Mono<InternalTopic> createTopic(KafkaCluster cluster, TopicCreationDTO creation) {
    return Mono.fromRunnable(() -> createTopicSync(cluster, creation))
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorMap(DataHubErrors::translate)
        .then(getTopicDetails(cluster, creation.getName()));
  }

  @Override
  public Mono<Void> deleteTopic(KafkaCluster cluster, String topicName) {
    return Mono.fromRunnable(() -> {
      DataHubTopicName id = DataHubTopicName.parse(cluster, topicName);
      clientFactory.get(cluster).deleteTopic(id.project(), id.topic());
    }).subscribeOn(Schedulers.boundedElastic())
        .onErrorMap(DataHubErrors::translate)
        .doOnSuccess(v -> statisticsCache.onTopicDelete(cluster, topicName))
        .then();
  }

  private void createTopicSync(KafkaCluster cluster, TopicCreationDTO creation) {
    DataHubTopicName id = DataHubTopicName.parse(cluster, creation.getName());
    Map<String, String> configs = Optional.ofNullable(creation.getConfigs()).orElse(Map.of());
    int shards = Optional.ofNullable(creation.getPartitions()).filter(p -> p > 0)
        .orElseThrow(() -> new ValidationException("Number of shards (partitions) must be positive"));
    int lifecycle = lifecycleDays(configs.get("retention.ms"));
    RecordType type = recordType(configs.getOrDefault(DataHubTopicSnapshot.CONFIG_RECORD_TYPE, "BLOB"));
    String comment = configs.getOrDefault(DataHubTopicSnapshot.CONFIG_COMMENT, "created by kafbat-ui");
    var client = clientFactory.get(cluster);
    if (type == RecordType.TUPLE) {
      RecordSchema schema = DataHubSchemas.parse(configs.get(DataHubTopicSnapshot.CONFIG_SCHEMA));
      client.createTopic(id.project(), id.topic(), shards, lifecycle, type, schema, comment);
    } else {
      client.createTopic(id.project(), id.topic(), shards, lifecycle, type, comment);
    }
    client.waitForShardReady(id.project(), id.topic());
  }

  private static RecordType recordType(String value) {
    try {
      return RecordType.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new ValidationException("datahub.record.type must be BLOB or TUPLE");
    }
  }

  static int lifecycleDays(String retentionMs) {
    if (retentionMs == null || retentionMs.isBlank()) {
      return DEFAULT_LIFECYCLE_DAYS;
    }
    long ms;
    try {
      ms = Long.parseLong(retentionMs.trim());
    } catch (NumberFormatException e) {
      throw new ValidationException("retention.ms should be a number of milliseconds");
    }
    if (ms <= 0) {
      throw new ValidationException("DataHub topics require a finite lifecycle, retention.ms must be positive");
    }
    long dayMs = Duration.ofDays(1).toMillis();
    return (int) Math.max(1, (ms + dayMs - 1) / dayMs);
  }
}
