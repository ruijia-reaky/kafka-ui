import { Cluster, ClusterFeaturesEnum } from 'generated-sources';

type Features = Cluster['features'];

export const isDataHubCluster = (features: Features) =>
  !!features?.includes(ClusterFeaturesEnum.DATAHUB_SHARDS);

// Kafka capabilities are only hidden for DataHub clusters, so Kafka clusters
// keep them while features are not loaded yet (e.g. cluster is offline).
const hasKafkaCapability = (features: Features, key: ClusterFeaturesEnum) =>
  !isDataHubCluster(features) || !!features?.includes(key);

export const isBrokersViewAvailable = (features: Features) =>
  hasKafkaCapability(features, ClusterFeaturesEnum.BROKERS_VIEW);

export const isTopicConfigEditAllowed = (features: Features) =>
  hasKafkaCapability(features, ClusterFeaturesEnum.TOPIC_CONFIGS_EDIT);

export const isTopicPartitionsEditAllowed = (features: Features) =>
  hasKafkaCapability(features, ClusterFeaturesEnum.TOPIC_PARTITIONS_EDIT);
