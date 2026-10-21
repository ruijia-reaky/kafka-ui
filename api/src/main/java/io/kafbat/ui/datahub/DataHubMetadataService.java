package io.kafbat.ui.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.GetCursorResult;
import com.aliyun.datahub.client.model.ListSubscriptionResult;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.SubscriptionEntry;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import io.kafbat.ui.config.ClustersProperties;
import io.kafbat.ui.model.InternalPartitionsOffsets;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.model.Metrics;
import io.kafbat.ui.model.ServerStatusDTO;
import io.kafbat.ui.model.Statistics;
import io.kafbat.ui.service.FeatureService;
import io.kafbat.ui.service.ReactiveAdminClient;
import io.kafbat.ui.service.StatisticsCache;
import io.kafbat.ui.service.metrics.scrape.ScrapedClusterState;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Service
@RequiredArgsConstructor
public class DataHubMetadataService {

  private static final int SUBSCRIPTIONS_PAGE_SIZE = 100;
  private static final int TOPICS_LOAD_CONCURRENCY = 8;

  private final DataHubClientFactory clientFactory;
  private final FeatureService featureService;
  private final ClustersProperties clustersProperties;

  public Mono<Statistics> loadStatistics(KafkaCluster cluster) {
    return Flux.fromIterable(cluster.getDatahub().getProjects())
        .flatMap(project -> blocking(() -> client(cluster).listTopic(project).getTopicNames())
            .flatMapIterable(Function.identity())
            .map(topic -> new DataHubTopicName(project, topic)))
        .flatMap(id -> loadTopic(cluster, id), TOPICS_LOAD_CONCURRENCY)
        .collectList()
        .map(snapshots -> toStatistics(cluster, snapshots))
        .doOnError(e -> log.error("Failed to collect DataHub cluster {} info: {}", cluster.getName(),
            DataHubErrors.describe(e)))
        .onErrorResume(e -> Mono.just(Statistics.statsUpdateError(e)));
  }

  public Mono<DataHubTopicSnapshot> loadTopic(KafkaCluster cluster, String uiName) {
    return Mono.fromCallable(() -> DataHubTopicName.parse(cluster, uiName))
        .flatMap(id -> loadTopic(cluster, id));
  }

  public Mono<DataHubTopicSnapshot> loadTopic(KafkaCluster cluster, DataHubTopicName id) {
    return blocking(() -> loadTopicSync(cluster, id, true, true))
        .onErrorMap(DataHubErrors::translate);
  }

  /**
   * Loads topic info, shards and offsets, without subscriptions.
   */
  public Mono<DataHubTopicSnapshot> loadTopicMetadata(KafkaCluster cluster, String uiName) {
    return Mono.fromCallable(() -> DataHubTopicName.parse(cluster, uiName))
        .flatMap(id -> blocking(() -> loadTopicSync(cluster, id, true, false)))
        .onErrorMap(DataHubErrors::translate);
  }

  /**
   * Loads topic info and shards only, offsets are left empty.
   */
  public Mono<DataHubTopicSnapshot> loadTopicShards(KafkaCluster cluster, String uiName) {
    return Mono.fromCallable(() -> DataHubTopicName.parse(cluster, uiName))
        .flatMap(id -> blocking(() -> loadTopicSync(cluster, id, false, false)))
        .onErrorMap(DataHubErrors::translate);
  }

  /**
   * Refreshes a single topic in the statistics cache, so pages read after a change see it immediately.
   */
  public Mono<DataHubTopicSnapshot> refreshTopic(KafkaCluster cluster, String uiName, StatisticsCache cache) {
    return loadTopic(cluster, uiName).doOnNext(s -> {
      Node node = node(cluster);
      cache.update(cluster,
          Map.of(s.name(), s.toDescription(node)),
          Map.of(s.name(), s.configs()),
          new InternalPartitionsOffsets(toOffsets(s)),
          clustersProperties);
      Statistics stats = cache.get(cluster);
      Map<String, ScrapedClusterState.ConsumerGroupState> groups =
          new HashMap<>(stats.getClusterState().getConsumerGroupsStates());
      groups.values().removeIf(g -> topicOf(g).filter(s.name()::equals).isPresent());
      s.subscriptions().forEach(sub -> groups.put(sub.groupId(), toGroupState(sub, node)));
      cache.replace(cluster, stats.withClusterState(st -> st.toBuilder().consumerGroupsStates(groups).build()));
    });
  }

  /**
   * Topic of a DataHub subscription group, taken from its single member assignment.
   */
  public static Optional<String> topicOf(ScrapedClusterState.ConsumerGroupState group) {
    return group.description().members().stream()
        .flatMap(m -> m.assignment().topicPartitions().stream())
        .map(TopicPartition::topic)
        .findFirst();
  }

  private static ScrapedClusterState.ConsumerGroupState toGroupState(DataHubTopicSnapshot.Subscription sub,
                                                                     Node node) {
    return new ScrapedClusterState.ConsumerGroupState(sub.groupId(), sub.toDescription(node), sub.committedOffsets());
  }

  public Node node(KafkaCluster cluster) {
    URI uri = URI.create(cluster.getDatahub().getEndpoint());
    int port = uri.getPort() > 0 ? uri.getPort() : ("http".equalsIgnoreCase(uri.getScheme()) ? 80 : 443);
    return new Node(0, uri.getHost() == null ? cluster.getDatahub().getEndpoint() : uri.getHost(), port);
  }

  private DatahubClient client(KafkaCluster cluster) {
    return clientFactory.get(cluster);
  }

  private DataHubTopicSnapshot loadTopicSync(KafkaCluster cluster, DataHubTopicName id,
                                             boolean withOffsets, boolean withSubscriptions) {
    DatahubClient client = client(cluster);
    var info = client.getTopic(id.project(), id.topic());
    List<ShardEntry> shards = client.listShard(id.project(), id.topic()).getShards();
    Map<Integer, Long> start = new HashMap<>();
    Map<Integer, Long> end = new HashMap<>();
    for (ShardEntry shard : withOffsets ? shards : List.<ShardEntry>of()) {
      try {
        GetCursorResult oldest = client.getCursor(id.project(), id.topic(), shard.getShardId(), CursorType.OLDEST);
        int partition = DataHubTopicSnapshot.partition(shard.getShardId());
        // an empty shard reports sequence 0 with timestamp 0 for both OLDEST and LATEST cursors
        if (oldest.getTimestamp() <= 0) {
          start.put(partition, oldest.getSequence());
          end.put(partition, oldest.getSequence());
        } else {
          GetCursorResult latest = client.getCursor(id.project(), id.topic(), shard.getShardId(), CursorType.LATEST);
          start.put(partition, oldest.getSequence());
          end.put(partition, latest.getSequence() + 1);
        }
      } catch (RuntimeException e) {
        log.debug("Offsets unavailable for {}/{} shard {}: {}", id.project(), id.topic(), shard.getShardId(),
            DataHubErrors.describe(e));
      }
    }
    String uiName = id.format(cluster);
    List<String> shardIds = shards.stream().map(ShardEntry::getShardId).toList();
    Set<TopicPartition> assignment = shardIds.stream()
        .map(s -> new TopicPartition(uiName, DataHubTopicSnapshot.partition(s)))
        .collect(Collectors.toSet());
    List<SubscriptionEntry> entries = withSubscriptions ? listSubscriptions(client, id) : List.of();
    List<DataHubTopicSnapshot.Subscription> subscriptions = entries.stream()
        .map(sub -> new DataHubTopicSnapshot.Subscription(
            DataHubTopicName.format(cluster, id.project(), sub.getSubId()),
            uiName,
            sub,
            assignment,
            committedOffsets(client, id, uiName, sub.getSubId(), shardIds, start)))
        .toList();
    return new DataHubTopicSnapshot(uiName, id, info, shards, start, end, subscriptions);
  }

  private List<SubscriptionEntry> listSubscriptions(DatahubClient client, DataHubTopicName id) {
    List<SubscriptionEntry> result = new ArrayList<>();
    for (int page = 1; ; page++) {
      ListSubscriptionResult res = client.listSubscription(id.project(), id.topic(), page, SUBSCRIPTIONS_PAGE_SIZE);
      result.addAll(res.getSubscriptions());
      if (res.getSubscriptions().size() < SUBSCRIPTIONS_PAGE_SIZE || result.size() >= res.getTotalCount()) {
        return result;
      }
    }
  }

  private Map<TopicPartition, Long> committedOffsets(DatahubClient client, DataHubTopicName id, String uiName,
                                                     String subId, List<String> shardIds,
                                                     Map<Integer, Long> startOffsets) {
    Map<String, SubscriptionOffset> offsets =
        client.getSubscriptionOffset(id.project(), id.topic(), subId, shardIds).getOffsets();
    return toCommittedOffsets(uiName, offsets == null ? Map.of() : offsets, startOffsets);
  }

  /**
   * DataHub stores the sequence of the last consumed record, a negative sequence means nothing was
   * consumed and the subscription starts from the oldest record of the shard.
   */
  static Map<TopicPartition, Long> toCommittedOffsets(String uiName, Map<String, SubscriptionOffset> offsets,
                                                      Map<Integer, Long> startOffsets) {
    Map<TopicPartition, Long> committed = new HashMap<>();
    startOffsets.forEach((partition, start) -> {
      SubscriptionOffset offset = offsets.get(String.valueOf(partition));
      long next = offset != null && offset.getSequence() >= 0 ? offset.getSequence() + 1 : start;
      committed.put(new TopicPartition(uiName, partition), Math.max(next, start));
    });
    return committed;
  }

  private Statistics toStatistics(KafkaCluster cluster, List<DataHubTopicSnapshot> snapshots) {
    Node node = node(cluster);
    Map<String, TopicDescription> descriptions = new HashMap<>();
    Map<String, List<ConfigEntry>> configs = new HashMap<>();
    Map<TopicPartition, InternalPartitionsOffsets.Offsets> offsets = new HashMap<>();
    Map<String, ScrapedClusterState.ConsumerGroupState> groups = new HashMap<>();
    for (DataHubTopicSnapshot s : snapshots) {
      descriptions.put(s.name(), s.toDescription(node));
      configs.put(s.name(), s.configs());
      offsets.putAll(toOffsets(s));
      s.subscriptions().forEach(sub -> groups.put(sub.groupId(), toGroupState(sub, node)));
    }
    ScrapedClusterState state = ScrapedClusterState.empty()
        .updateTopics(descriptions, configs, new InternalPartitionsOffsets(offsets), clustersProperties)
        .toBuilder()
        .consumerGroupsStates(groups)
        .build();
    return Statistics.builder()
        .status(ServerStatusDTO.ONLINE)
        .version("DataHub")
        .features(featureService.getDataHubFeatures())
        .clusterDescription(new ReactiveAdminClient.ClusterDescription(
            null, cluster.getDatahub().getEndpoint(), List.of(), null))
        .metrics(Metrics.empty())
        .clusterState(state)
        .connectStates(Map.of())
        .build();
  }

  static Map<TopicPartition, InternalPartitionsOffsets.Offsets> toOffsets(DataHubTopicSnapshot s) {
    return s.startOffsets().keySet().stream()
        .filter(p -> s.endOffsets().containsKey(p))
        .collect(Collectors.toMap(
            p -> new TopicPartition(s.name(), p),
            p -> new InternalPartitionsOffsets.Offsets(s.startOffsets().get(p), s.endOffsets().get(p))));
  }

  private static <T> Mono<T> blocking(java.util.concurrent.Callable<T> call) {
    return Mono.fromCallable(call).subscribeOn(Schedulers.boundedElastic());
  }
}
