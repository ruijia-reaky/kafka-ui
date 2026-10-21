import { ClusterFeaturesEnum } from 'generated-sources';
import {
  isBrokersViewAvailable,
  isDataHubCluster,
  isTopicConfigEditAllowed,
  isTopicPartitionsEditAllowed,
} from 'lib/clusterFeatures';

describe('clusterFeatures', () => {
  it('keeps Kafka capabilities when features are not loaded', () => {
    expect(isDataHubCluster(undefined)).toBe(false);
    expect(isBrokersViewAvailable(undefined)).toBe(true);
    expect(isTopicConfigEditAllowed([])).toBe(true);
    expect(isTopicPartitionsEditAllowed([])).toBe(true);
  });

  it('hides Kafka capabilities for DataHub clusters', () => {
    const features = [
      ClusterFeaturesEnum.DATAHUB_SHARDS,
      ClusterFeaturesEnum.TOPIC_DELETION,
    ];
    expect(isDataHubCluster(features)).toBe(true);
    expect(isBrokersViewAvailable(features)).toBe(false);
    expect(isTopicConfigEditAllowed(features)).toBe(false);
    expect(isTopicPartitionsEditAllowed(features)).toBe(false);
  });

  it('respects capabilities reported by the backend', () => {
    const features = [
      ClusterFeaturesEnum.DATAHUB_SHARDS,
      ClusterFeaturesEnum.TOPIC_CONFIGS_EDIT,
    ];
    expect(isTopicConfigEditAllowed(features)).toBe(true);
    expect(isTopicPartitionsEditAllowed(features)).toBe(false);
  });
});
