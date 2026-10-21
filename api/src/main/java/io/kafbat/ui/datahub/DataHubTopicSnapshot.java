package io.kafbat.ui.datahub;

import com.aliyun.datahub.client.model.GetTopicResult;
import com.aliyun.datahub.client.model.RecordType;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.ShardState;
import com.aliyun.datahub.client.model.SubscriptionEntry;
import com.aliyun.datahub.client.model.SubscriptionState;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

/**
 * DataHub topic state expressed in Kafka terms: shard id = partition, sequence = offset.
 * End offsets follow Kafka semantics (sequence of the next record to be written).
 */
public record DataHubTopicSnapshot(String name,
                                   DataHubTopicName id,
                                   GetTopicResult info,
                                   List<ShardEntry> shards,
                                   Map<Integer, Long> startOffsets,
                                   Map<Integer, Long> endOffsets,
                                   List<Subscription> subscriptions) {

  public static final String CONFIG_PROJECT = "datahub.project";
  public static final String CONFIG_RECORD_TYPE = "datahub.record.type";
  public static final String CONFIG_SCHEMA = "datahub.schema";
  public static final String CONFIG_COMMENT = "datahub.comment";
  public static final String CONFIG_EXPAND_MODE = "datahub.expand.mode";
  public static final String CONFIG_CREATE_TIME = "datahub.create.time";
  public static final String SUBSCRIPTION_ASSIGNOR = "datahub-subscription";

  /**
   * Committed offsets use Kafka semantics: the next sequence to consume.
   * A subscription is presented as a group with one member assigned to all shards of its topic.
   */
  public record Subscription(String groupId,
                             String topic,
                             SubscriptionEntry entry,
                             Set<TopicPartition> assignment,
                             Map<TopicPartition, Long> committedOffsets) {

    public ConsumerGroupDescription toDescription(Node coordinator) {
      ConsumerGroupState state = entry.getState() == SubscriptionState.OFFLINE
          ? ConsumerGroupState.DEAD
          : ConsumerGroupState.STABLE;
      var member = new MemberDescription(entry.getSubId(), SUBSCRIPTION_ASSIGNOR, coordinator.host(),
          new MemberAssignment(assignment));
      return new ConsumerGroupDescription(groupId, true, List.of(member), SUBSCRIPTION_ASSIGNOR, state, coordinator);
    }
  }

  public static int partition(String shardId) {
    return Integer.parseInt(shardId);
  }

  public boolean isTuple() {
    return info.getRecordType() == RecordType.TUPLE;
  }

  public TopicDescription toDescription(Node node) {
    List<TopicPartitionInfo> partitions = shards.stream()
        .map(s -> new TopicPartitionInfo(
            partition(s.getShardId()),
            s.getState() == ShardState.ACTIVE ? node : null,
            List.of(node),
            s.getState() == ShardState.ACTIVE ? List.of(node) : List.of()))
        .sorted((a, b) -> Integer.compare(a.partition(), b.partition()))
        .toList();
    return new TopicDescription(name, false, partitions);
  }

  public List<ConfigEntry> configs() {
    List<ConfigEntry> configs = new ArrayList<>();
    configs.add(new ConfigEntry("cleanup.policy", "delete"));
    configs.add(new ConfigEntry("retention.ms", String.valueOf(Duration.ofDays(info.getLifeCycle()).toMillis())));
    configs.add(new ConfigEntry(CONFIG_PROJECT, id.project()));
    configs.add(new ConfigEntry(CONFIG_RECORD_TYPE, String.valueOf(info.getRecordType())));
    if (isTuple()) {
      configs.add(new ConfigEntry(CONFIG_SCHEMA, DataHubSchemas.format(info.getRecordSchema())));
    }
    configs.add(new ConfigEntry(CONFIG_COMMENT, info.getComment() == null ? "" : info.getComment()));
    if (info.getExpandMode() != null) {
      configs.add(new ConfigEntry(CONFIG_EXPAND_MODE, info.getExpandMode().name()));
    }
    if (info.getCreateTime() > 0) {
      configs.add(new ConfigEntry(CONFIG_CREATE_TIME, Instant.ofEpochSecond(info.getCreateTime()).toString()));
    }
    return configs;
  }
}
