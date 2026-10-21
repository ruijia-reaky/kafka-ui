import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { dataHubApiClient as api } from 'lib/api';
import { showSuccessAlert } from 'lib/errorHandling';
import {
  DataHubShardMerge,
  DataHubShardSplit,
  GetDataHubShardsRequest,
} from 'generated-sources';
import { topicKeys } from 'lib/hooks/api/topics';

const shardKeys = (props: GetDataHubShardsRequest) =>
  [...topicKeys.details(props), 'datahub-shards'] as const;

export function useDataHubShards(props: GetDataHubShardsRequest) {
  return useQuery({
    queryKey: shardKeys(props),
    queryFn: () => api.getDataHubShards(props),
  });
}

function useShardsMutation<T>(
  props: GetDataHubShardsRequest,
  mutationFn: (payload: T) => Promise<unknown>,
  message: string
) {
  const client = useQueryClient();
  return useMutation({
    mutationFn,
    onSuccess: () => {
      showSuccessAlert({ message });
      client.invalidateQueries({ queryKey: topicKeys.details(props) });
    },
  });
}

export function useSplitDataHubShard(props: GetDataHubShardsRequest) {
  return useShardsMutation(
    props,
    (split: DataHubShardSplit) =>
      api.splitDataHubShard({ ...props, dataHubShardSplit: split }),
    'Shard successfully split'
  );
}

export function useMergeDataHubShards(props: GetDataHubShardsRequest) {
  return useShardsMutation(
    props,
    (merge: DataHubShardMerge) =>
      api.mergeDataHubShards({ ...props, dataHubShardMerge: merge }),
    'Shards successfully merged'
  );
}
