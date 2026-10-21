package io.kafbat.ui.datahub;

import com.aliyun.datahub.client.exception.SeekOutOfRangeException;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.SubscriptionOffset;
import io.kafbat.ui.exception.NotFoundException;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.ConsumerGroupOrderingDTO;
import io.kafbat.ui.model.ConsumerGroupStateDTO;
import io.kafbat.ui.model.InternalConsumerGroup;
import io.kafbat.ui.model.InternalTopicConsumerGroup;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.model.SortOrderDTO;
import io.kafbat.ui.service.ConsumerGroupService.ConsumerGroupsPage;
import io.kafbat.ui.service.StatisticsCache;
import io.kafbat.ui.service.backend.ConsumerGroupOperations;
import io.kafbat.ui.service.backend.OffsetsResetOperations;
import io.kafbat.ui.service.metrics.scrape.ScrapedClusterState;
import io.kafbat.ui.service.rbac.AccessControlService;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * DataHub subscriptions exposed as consumer groups. Group id is the subscription id
 * ({@code project.subId} when several projects are configured).
 */
@Service
@RequiredArgsConstructor
public class DataHubConsumerGroupService implements ConsumerGroupOperations, OffsetsResetOperations {

  private final DataHubClientFactory clientFactory;
  private final DataHubMetadataService metadataService;
  private final StatisticsCache statisticsCache;
  private final AccessControlService accessControlService;

  @Override
  public Mono<ConsumerGroupsPage> getConsumerGroups(KafkaCluster cluster,
                                                    OptionalInt pageNum,
                                                    OptionalInt perPage,
                                                    @Nullable String search,
                                                    Boolean fts,
                                                    ConsumerGroupOrderingDTO orderBy,
                                                    SortOrderDTO sortOrderDto,
                                                    List<ConsumerGroupStateDTO> states) {
    var clusterState = statisticsCache.get(cluster).getClusterState();
    var stateNames = states.stream().map(Enum::name).collect(Collectors.toSet());
    return Flux.fromIterable(clusterState.getConsumerGroupsStates().values())
        .filter(g -> search == null || g.group().toLowerCase(Locale.ROOT).contains(search.toLowerCase(Locale.ROOT)))
        .filter(g -> stateNames.isEmpty() || stateNames.contains(g.description().state().name()))
        .filterWhen(g -> accessControlService.isConsumerGroupAccessible(g.group(), cluster.getName()))
        .map(g -> toInternal(g, clusterState))
        .collectList()
        .map(groups -> {
          Comparator<InternalConsumerGroup> comparator = comparator(orderBy);
          var sorted = groups.stream()
              .sorted(sortOrderDto == SortOrderDTO.DESC ? comparator.reversed() : comparator);
          if (pageNum.isPresent() && perPage.isPresent()) {
            sorted = sorted.skip((long) (pageNum.getAsInt() - 1) * perPage.getAsInt()).limit(perPage.getAsInt());
          }
          return ConsumerGroupsPage.from(sorted.toList(), groups.size(), pageNum, perPage);
        });
  }

  @Override
  public Mono<List<InternalTopicConsumerGroup>> getConsumerGroupsForTopic(KafkaCluster cluster, String topic) {
    return metadataService.refreshTopic(cluster, topic, statisticsCache)
        .map(snapshot -> {
          Map<TopicPartition, Long> endOffsets = endOffsets(snapshot);
          Node coordinator = metadataService.node(cluster);
          return snapshot.subscriptions().stream()
              .map(sub -> InternalTopicConsumerGroup.create(
                  topic, sub.toDescription(coordinator), sub.committedOffsets(), endOffsets))
              .toList();
        });
  }

  @Override
  public Mono<InternalConsumerGroup> getConsumerGroupDetail(KafkaCluster cluster, String consumerGroupId) {
    return refreshGroupTopic(cluster, consumerGroupId)
        .map(t -> {
          var clusterState = statisticsCache.get(cluster).getClusterState();
          var group = clusterState.getConsumerGroupsStates().get(consumerGroupId);
          if (group == null) {
            throw notFound(consumerGroupId);
          }
          return toInternal(group, clusterState);
        });
  }

  @Override
  public Mono<Void> deleteConsumerGroupById(KafkaCluster cluster, String groupId) {
    return Mono.defer(() -> {
      String topic = topicOf(cluster, groupId);
      DataHubTopicName sub = DataHubTopicName.parse(cluster, groupId);
      DataHubTopicName topicId = DataHubTopicName.parse(cluster, topic);
      return Mono.fromRunnable(() ->
              clientFactory.get(cluster).deleteSubscription(topicId.project(), topicId.topic(), sub.topic()))
          .subscribeOn(Schedulers.boundedElastic())
          .onErrorMap(DataHubErrors::translate)
          .then(metadataService.refreshTopic(cluster, topic, statisticsCache))
          .then();
    });
  }

  @Override
  public Mono<Void> resetToEarliest(KafkaCluster cluster, String group, String topic,
                                    Collection<Integer> partitions) {
    return reset(cluster, group, topic, snapshot -> targets(snapshot, partitions, snapshot.startOffsets()));
  }

  @Override
  public Mono<Void> resetToLatest(KafkaCluster cluster, String group, String topic,
                                  Collection<Integer> partitions) {
    return reset(cluster, group, topic, snapshot -> targets(snapshot, partitions, snapshot.endOffsets()));
  }

  @Override
  public Mono<Void> resetToTimestamp(KafkaCluster cluster, String group, String topic,
                                     Collection<Integer> partitions, long targetTimestamp) {
    return reset(cluster, group, topic, snapshot -> {
      var client = clientFactory.get(cluster);
      Map<Integer, Long> found = new HashMap<>();
      for (var target : targets(snapshot, partitions, snapshot.endOffsets()).entrySet()) {
        int p = target.getKey();
        try {
          found.put(p, client.getCursor(snapshot.id().project(), snapshot.id().topic(), String.valueOf(p),
              CursorType.SYSTEM_TIME, targetTimestamp).getSequence());
        } catch (SeekOutOfRangeException e) {
          found.put(p, targetTimestamp > System.currentTimeMillis()
              ? target.getValue() : snapshot.startOffsets().getOrDefault(p, 0L));
        }
      }
      return found;
    });
  }

  @Override
  public Mono<Void> resetToOffsets(KafkaCluster cluster, String group, String topic, Map<Integer, Long> offsets) {
    return reset(cluster, group, topic, snapshot -> offsets);
  }

  private Mono<Void> reset(KafkaCluster cluster, String group, String topic,
                           Function<DataHubTopicSnapshot, Map<Integer, Long>> targetOffsets) {
    return metadataService.loadTopic(cluster, topic)
        .publishOn(Schedulers.boundedElastic())
        .map(snapshot -> {
          var subscription = snapshot.subscriptions().stream()
              .filter(s -> s.groupId().equals(group))
              .findFirst()
              .orElseThrow(() -> notFound(group));
          Map<String, SubscriptionOffset> request = toSubscriptionOffsets(snapshot, targetOffsets.apply(snapshot));
          clientFactory.get(cluster).resetSubscriptionOffset(
              snapshot.id().project(), snapshot.id().topic(), subscription.entry().getSubId(), request);
          return snapshot;
        })
        .onErrorMap(e -> e instanceof NotFoundException || e instanceof ValidationException
            ? e : DataHubErrors.translate(e))
        .then(metadataService.refreshTopic(cluster, topic, statisticsCache))
        .then();
  }

  /**
   * Kafka offsets (next record to consume) clamped to shard bounds and converted to DataHub
   * subscription offsets (sequence of the last consumed record).
   */
  static Map<String, SubscriptionOffset> toSubscriptionOffsets(DataHubTopicSnapshot snapshot,
                                                               Map<Integer, Long> kafkaOffsets) {
    Map<String, SubscriptionOffset> result = new HashMap<>();
    kafkaOffsets.forEach((partition, offset) -> {
      Long start = snapshot.startOffsets().get(partition);
      Long end = snapshot.endOffsets().get(partition);
      if (start == null || end == null) {
        throw new ValidationException("Shard %d offsets are not available".formatted(partition));
      }
      long target = Math.min(Math.max(offset, start), end);
      SubscriptionOffset subscriptionOffset = new SubscriptionOffset();
      subscriptionOffset.setSequence(target - 1);
      subscriptionOffset.setTimestamp(-1);
      result.put(String.valueOf(partition), subscriptionOffset);
    });
    return result;
  }

  private static Map<Integer, Long> targets(DataHubTopicSnapshot snapshot, @Nullable Collection<Integer> partitions,
                                            Map<Integer, Long> offsets) {
    return offsets.entrySet().stream()
        .filter(e -> partitions == null || partitions.isEmpty() || partitions.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Mono<DataHubTopicSnapshot> refreshGroupTopic(KafkaCluster cluster, String groupId) {
    return Mono.fromCallable(() -> topicOf(cluster, groupId))
        .flatMap(topic -> metadataService.refreshTopic(cluster, topic, statisticsCache));
  }

  private String topicOf(KafkaCluster cluster, String groupId) {
    var group = statisticsCache.get(cluster).getClusterState().getConsumerGroupsStates().get(groupId);
    if (group == null) {
      throw notFound(groupId);
    }
    return DataHubMetadataService.topicOf(group).orElseThrow(() -> notFound(groupId));
  }

  private static InternalConsumerGroup toInternal(ScrapedClusterState.ConsumerGroupState group,
                                                  ScrapedClusterState clusterState) {
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    DataHubMetadataService.topicOf(group)
        .map(t -> clusterState.getTopicStates().get(t))
        .ifPresent(t -> t.endOffsets().forEach((p, o) -> endOffsets.put(new TopicPartition(t.name(), p), o)));
    endOffsets.keySet().retainAll(group.committedOffsets().keySet());
    return InternalConsumerGroup.create(group.description(), group.committedOffsets(), endOffsets);
  }

  private static Map<TopicPartition, Long> endOffsets(DataHubTopicSnapshot snapshot) {
    return snapshot.endOffsets().entrySet().stream()
        .collect(Collectors.toMap(e -> new TopicPartition(snapshot.name(), e.getKey()), Map.Entry::getValue));
  }

  private static Comparator<InternalConsumerGroup> comparator(ConsumerGroupOrderingDTO orderBy) {
    return switch (orderBy) {
      case STATE -> Comparator.comparing(g -> g.getState().name());
      case MEMBERS -> Comparator.comparingInt(g -> g.getMembers().size());
      case MESSAGES_BEHIND -> Comparator.comparingLong(g -> g.getConsumerLag() == null ? 0L : g.getConsumerLag());
      case TOPIC_NUM -> Comparator.comparingInt(InternalConsumerGroup::getTopicNum);
      default -> Comparator.comparing(InternalConsumerGroup::getGroupId);
    };
  }

  private static NotFoundException notFound(String groupId) {
    return new NotFoundException("Subscription '%s' not found".formatted(groupId));
  }
}
