import "@typespec/openapi";
import "./responses.tsp";

namespace Api;

using TypeSpec.Http;
using OpenAPI;

@route("/api/clusters/{clusterName}/topics/{topicName}/datahub/shards")
@tag("DataHub")
interface DataHubApi {
  @get
  @operationId("getDataHubShards")
  @summary("getDataHubShards")
  getDataHubShards(
    @path clusterName: string,
    @path topicName: string,
  ): DataHubShard[] | ApiNotFoundResponse;

  @post
  @route("/split")
  @operationId("splitDataHubShard")
  @summary("splitDataHubShard")
  splitDataHubShard(
    @path clusterName: string,
    @path topicName: string,
    @body split: DataHubShardSplit,
  ): DataHubShard[] | ApiBadRequestResponse | ApiNotFoundResponse;

  @post
  @route("/merge")
  @operationId("mergeDataHubShards")
  @summary("mergeDataHubShards")
  mergeDataHubShards(
    @path clusterName: string,
    @path topicName: string,
    @body merge: DataHubShardMerge,
  ): DataHubShard[] | ApiBadRequestResponse | ApiNotFoundResponse;
}

enum DataHubShardState {
  OPENING,
  ACTIVE,
  CLOSING,
  CLOSED,
  INACTIVE,
}

model DataHubShard {
  shardId: string;
  partition: int32;
  state: DataHubShardState;
  beginHashKey?: string;
  endHashKey?: string;
  parentShardIds?: string[];
  leftShardId?: string;
  rightShardId?: string;
  @doc("Close time, unix timestamp in seconds")
  closedTime?: int64;
  offsetMin?: int64;
  offsetMax?: int64;
}

model DataHubShardSplit {
  shardId: string;

  @doc("Hash key (32 hex chars) to split at, middle of the shard hash range if not set")
  splitKey?: string;
}

model DataHubShardMerge {
  shardId: string;

  @doc("Shard adjacent to shardId by hash range")
  adjacentShardId: string;
}
