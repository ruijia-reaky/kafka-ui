import React from 'react';
import { ColumnDef, CellContext } from '@tanstack/react-table';
import {
  Action,
  DataHubShard,
  DataHubShardState,
  ResourceType,
} from 'generated-sources';
import Table, { TagCell } from 'components/common/NewTable';
import { Dropdown } from 'components/common/Dropdown';
import { ActionDropdownItem } from 'components/common/ActionComponent';
import ClusterContext from 'components/contexts/ClusterContext';
import PageLoader from 'components/common/PageLoader/PageLoader';
import useAppParams from 'lib/hooks/useAppParams';
import { RouteParamsClusterTopic } from 'lib/paths';
import {
  useDataHubShards,
  useMergeDataHubShards,
  useSplitDataHubShard,
} from 'lib/hooks/api/datahub';

// Shards are adjacent when the hash range of one ends where the other begins.
export const findRightNeighbour = (
  shard: DataHubShard,
  shards: DataHubShard[]
) =>
  shards.find(
    (s) =>
      s.state === DataHubShardState.ACTIVE &&
      s.shardId !== shard.shardId &&
      !!shard.endHashKey &&
      s.beginHashKey === shard.endHashKey
  );

const ShardActionsCell: React.FC<
  CellContext<DataHubShard, unknown> & { shards: DataHubShard[] }
> = ({ row, shards }) => {
  const { clusterName, topicName } = useAppParams<RouteParamsClusterTopic>();
  const { isReadOnly } = React.useContext(ClusterContext);
  const split = useSplitDataHubShard({ clusterName, topicName });
  const merge = useMergeDataHubShards({ clusterName, topicName });
  const shard = row.original;
  const neighbour = findRightNeighbour(shard, shards);
  const isActive = shard.state === DataHubShardState.ACTIVE;
  const isPending = split.isPending || merge.isPending;
  const permission = {
    resource: ResourceType.TOPIC,
    action: Action.EDIT,
    value: topicName,
  };

  return (
    <Dropdown disabled={isReadOnly || !isActive}>
      <ActionDropdownItem
        onClick={() => split.mutateAsync({ shardId: shard.shardId })}
        confirm={
          <>
            Are you sure you want to split shard <b>{shard.shardId}</b>?
          </>
        }
        disabled={isPending}
        permission={permission}
      >
        Split
      </ActionDropdownItem>
      <ActionDropdownItem
        onClick={() =>
          neighbour &&
          merge.mutateAsync({
            shardId: shard.shardId,
            adjacentShardId: neighbour.shardId,
          })
        }
        confirm={
          <>
            Are you sure you want to merge shard <b>{shard.shardId}</b> with
            shard <b>{neighbour?.shardId}</b>?
          </>
        }
        disabled={isPending || !neighbour}
        danger
        permission={permission}
      >
        Merge with next shard
      </ActionDropdownItem>
    </Dropdown>
  );
};

const Shards: React.FC = () => {
  const { clusterName, topicName } = useAppParams<RouteParamsClusterTopic>();
  const { data: shards = [], isLoading } = useDataHubShards({
    clusterName,
    topicName,
  });

  const columns = React.useMemo<ColumnDef<DataHubShard>[]>(
    () => [
      { header: 'Shard ID', accessorKey: 'shardId' },
      { header: 'State', accessorKey: 'state', cell: TagCell },
      { header: 'Begin Hash Key', accessorKey: 'beginHashKey' },
      { header: 'End Hash Key', accessorKey: 'endHashKey' },
      {
        header: 'Parents',
        accessorKey: 'parentShardIds',
        cell: ({ getValue }) =>
          (getValue<DataHubShard['parentShardIds']>() || []).join(', '),
      },
      { header: 'First Offset', accessorKey: 'offsetMin' },
      { header: 'Next Offset', accessorKey: 'offsetMax' },
      {
        id: 'actions',
        header: '',
        // eslint-disable-next-line react/no-unstable-nested-components
        cell: (props) => <ShardActionsCell {...props} shards={shards} />,
      },
    ],
    [shards]
  );

  if (isLoading) {
    return <PageLoader />;
  }

  return (
    <Table
      columns={columns}
      data={shards}
      enableSorting
      emptyMessage="No shards found"
    />
  );
};

export default Shards;
