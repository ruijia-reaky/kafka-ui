package io.kafbat.ui.controller;

import static io.kafbat.ui.model.rbac.permission.TopicAction.EDIT;
import static io.kafbat.ui.model.rbac.permission.TopicAction.VIEW;

import io.kafbat.ui.api.DataHubApi;
import io.kafbat.ui.datahub.DataHubShardService;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.DataHubShardDTO;
import io.kafbat.ui.model.DataHubShardMergeDTO;
import io.kafbat.ui.model.DataHubShardSplitDTO;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.model.rbac.AccessContext;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
public class DataHubController extends AbstractController implements DataHubApi {

  private final DataHubShardService shardService;

  @Override
  public Mono<ResponseEntity<Flux<DataHubShardDTO>>> getDataHubShards(String clusterName, String topicName,
                                                                      ServerWebExchange exchange) {
    var context = AccessContext.builder()
        .cluster(clusterName)
        .topicActions(topicName, VIEW)
        .operationName("getDataHubShards")
        .build();
    return validateAccess(context)
        .then(Mono.defer(() -> shardService.getShards(dataHubCluster(clusterName), topicName)))
        .map(shards -> ResponseEntity.ok(Flux.fromIterable(shards)))
        .doOnEach(sig -> audit(context, sig));
  }

  @Override
  public Mono<ResponseEntity<Flux<DataHubShardDTO>>> splitDataHubShard(String clusterName, String topicName,
                                                                       Mono<DataHubShardSplitDTO> split,
                                                                       ServerWebExchange exchange) {
    var context = AccessContext.builder()
        .cluster(clusterName)
        .topicActions(topicName, VIEW, EDIT)
        .operationName("splitDataHubShard")
        .build();
    return validateAccess(context)
        .then(split)
        .flatMap(s -> shardService.split(dataHubCluster(clusterName), topicName, s))
        .map(shards -> ResponseEntity.ok(Flux.fromIterable(shards)))
        .doOnEach(sig -> audit(context, sig));
  }

  @Override
  public Mono<ResponseEntity<Flux<DataHubShardDTO>>> mergeDataHubShards(String clusterName, String topicName,
                                                                        Mono<DataHubShardMergeDTO> merge,
                                                                        ServerWebExchange exchange) {
    var context = AccessContext.builder()
        .cluster(clusterName)
        .topicActions(topicName, VIEW, EDIT)
        .operationName("mergeDataHubShards")
        .build();
    return validateAccess(context)
        .then(merge)
        .flatMap(m -> shardService.merge(dataHubCluster(clusterName), topicName, m))
        .map(shards -> ResponseEntity.ok(Flux.fromIterable(shards)))
        .doOnEach(sig -> audit(context, sig));
  }

  private KafkaCluster dataHubCluster(String clusterName) {
    KafkaCluster cluster = getCluster(clusterName);
    if (!cluster.isDataHub()) {
      throw new ValidationException("Cluster '%s' is not a DataHub cluster".formatted(clusterName));
    }
    return cluster;
  }
}
