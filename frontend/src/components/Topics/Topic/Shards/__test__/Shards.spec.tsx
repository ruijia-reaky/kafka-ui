import React from 'react';
import { screen } from '@testing-library/react';
import { render, WithRoute } from 'lib/testHelpers';
import { DataHubShard, DataHubShardState } from 'generated-sources';
import { clusterTopicPath } from 'lib/paths';
import Shards, {
  findRightNeighbour,
} from 'components/Topics/Topic/Shards/Shards';
import {
  useDataHubShards,
  useMergeDataHubShards,
  useSplitDataHubShard,
} from 'lib/hooks/api/datahub';

jest.mock('lib/hooks/api/datahub', () => ({
  useDataHubShards: jest.fn(),
  useSplitDataHubShard: jest.fn(),
  useMergeDataHubShards: jest.fn(),
}));

const shards: DataHubShard[] = [
  {
    shardId: '0',
    partition: 0,
    state: DataHubShardState.ACTIVE,
    beginHashKey: '00000000000000000000000000000000',
    endHashKey: '7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
    offsetMin: 0,
    offsetMax: 10,
  },
  {
    shardId: '1',
    partition: 1,
    state: DataHubShardState.ACTIVE,
    beginHashKey: '7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
    endHashKey: 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
    offsetMin: 3,
    offsetMax: 3,
  },
  {
    shardId: '2',
    partition: 2,
    state: DataHubShardState.CLOSED,
    beginHashKey: 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
    parentShardIds: ['0', '1'],
  },
];

describe('Shards', () => {
  it('finds the active shard adjacent by hash range', () => {
    expect(findRightNeighbour(shards[0], shards)?.shardId).toBe('1');
    expect(findRightNeighbour(shards[1], shards)).toBeUndefined();
  });

  it('renders shards with offsets and parents', () => {
    (useDataHubShards as jest.Mock).mockReturnValue({
      data: shards,
      isLoading: false,
    });
    (useSplitDataHubShard as jest.Mock).mockReturnValue({ isPending: false });
    (useMergeDataHubShards as jest.Mock).mockReturnValue({ isPending: false });
    render(
      <WithRoute path={clusterTopicPath()}>
        <Shards />
      </WithRoute>,
      { initialEntries: [clusterTopicPath('dh', 'meters')] }
    );
    expect(screen.getByText('Shard ID')).toBeInTheDocument();
    expect(screen.getAllByText('ACTIVE')).toHaveLength(2);
    expect(screen.getByText('CLOSED')).toBeInTheDocument();
    expect(screen.getByText('0, 1')).toBeInTheDocument();
  });
});
