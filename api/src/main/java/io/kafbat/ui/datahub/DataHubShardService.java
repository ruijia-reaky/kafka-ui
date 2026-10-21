package io.kafbat.ui.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.ShardEntry;
import io.kafbat.ui.model.DataHubShardDTO;
import io.kafbat.ui.model.DataHubShardMergeDTO;
import io.kafbat.ui.model.DataHubShardSplitDTO;
import io.kafbat.ui.model.DataHubShardStateDTO;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.StatisticsCache;
import java.util.Comparator;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
public class DataHubShardService {

  private final DataHubClientFactory clientFactory;
  private final DataHubMetadataService metadataService;
  private final StatisticsCache statisticsCache;

  public Mono<List<DataHubShardDTO>> getShards(KafkaCluster cluster, String topic) {
    return metadataService.loadTopicMetadata(cluster, topic).map(DataHubShardService::toDto);
  }

  public Mono<List<DataHubShardDTO>> split(KafkaCluster cluster, String topic, DataHubShardSplitDTO split) {
    return change(cluster, topic, (client, id) -> {
      if (StringUtils.hasText(split.getSplitKey())) {
        client.splitShard(id.project(), id.topic(), split.getShardId(), split.getSplitKey());
      } else {
        client.splitShard(id.project(), id.topic(), split.getShardId());
      }
    });
  }

  public Mono<List<DataHubShardDTO>> merge(KafkaCluster cluster, String topic, DataHubShardMergeDTO merge) {
    return change(cluster, topic,
        (client, id) -> client.mergeShard(id.project(), id.topic(), merge.getShardId(), merge.getAdjacentShardId()));
  }

  private Mono<List<DataHubShardDTO>> change(KafkaCluster cluster, String topic, ShardChange change) {
    return Mono.fromCallable(() -> DataHubTopicName.parse(cluster, topic))
        .publishOn(Schedulers.boundedElastic())
        .doOnNext(id -> {
          DatahubClient client = clientFactory.get(cluster);
          change.apply(client, id);
          client.waitForShardReady(id.project(), id.topic());
        })
        .onErrorMap(DataHubErrors::translate)
        .then(metadataService.refreshTopic(cluster, topic, statisticsCache))
        .map(DataHubShardService::toDto);
  }

  static List<DataHubShardDTO> toDto(DataHubTopicSnapshot snapshot) {
    return snapshot.shards().stream()
        .map(s -> toDto(snapshot, s))
        .sorted(Comparator.comparing(DataHubShardDTO::getPartition))
        .toList();
  }

  private static DataHubShardDTO toDto(DataHubTopicSnapshot snapshot, ShardEntry shard) {
    int partition = DataHubTopicSnapshot.partition(shard.getShardId());
    return new DataHubShardDTO()
        .shardId(shard.getShardId())
        .partition(partition)
        .state(DataHubShardStateDTO.fromValue(shard.getState().name()))
        .beginHashKey(shard.getBeginHashKey())
        .endHashKey(shard.getEndHashKey())
        .parentShardIds(shard.getParentShardIds())
        .leftShardId(shard.getLeftShardId())
        .rightShardId(shard.getRightShardId())
        .closedTime(shard.getClosedTime() > 0 ? shard.getClosedTime() : null)
        .offsetMin(snapshot.startOffsets().get(partition))
        .offsetMax(snapshot.endOffsets().get(partition));
  }

  @FunctionalInterface
  private interface ShardChange {
    void apply(DatahubClient client, DataHubTopicName id);
  }
}
