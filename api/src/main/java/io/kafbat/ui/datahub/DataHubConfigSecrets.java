package io.kafbat.ui.datahub;

import io.kafbat.ui.config.ClustersProperties;
import io.kafbat.ui.model.ApplicationConfigPropertiesDTO;
import io.kafbat.ui.util.DynamicConfigOperations;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * DataHub credentials are never returned by the config API. When a config with masked values is
 * submitted back, the masked values are replaced with the currently configured ones.
 */
public final class DataHubConfigSecrets {

  public static final String MASK = "******";

  private DataHubConfigSecrets() {
  }

  public static ApplicationConfigPropertiesDTO mask(ApplicationConfigPropertiesDTO dto) {
    Optional.ofNullable(dto.getKafka())
        .map(k -> k.getClusters())
        .ifPresent(clusters -> clusters.stream()
            .map(c -> c.getDatahub())
            .filter(Objects::nonNull)
            .forEach(dh -> {
              dh.setAccessKey(maskValue(dh.getAccessKey()));
              dh.setSecurityToken(maskValue(dh.getSecurityToken()));
            }));
    return dto;
  }

  public static DynamicConfigOperations.PropertiesStructure restore(
      DynamicConfigOperations.PropertiesStructure submitted,
      DynamicConfigOperations.PropertiesStructure current) {
    if (submitted.getKafka() == null || submitted.getKafka().getClusters() == null) {
      return submitted;
    }
    List<ClustersProperties.Cluster> currentClusters = Optional.ofNullable(current.getKafka())
        .map(ClustersProperties::getClusters)
        .stream()
        .flatMap(List::stream)
        .filter(c -> c.getName() != null && c.getDatahub() != null)
        .toList();
    Map<String, ClustersProperties.DataHubConfig> currentConfigs = currentClusters.stream()
        .collect(Collectors.toMap(ClustersProperties.Cluster::getName, ClustersProperties.Cluster::getDatahub,
            (a, b) -> a));
    for (ClustersProperties.Cluster cluster : submitted.getKafka().getClusters()) {
      ClustersProperties.DataHubConfig dh = cluster.getDatahub();
      if (dh == null) {
        continue;
      }
      @Nullable ClustersProperties.DataHubConfig existing = Optional
          .ofNullable(currentConfigs.get(cluster.getName()))
          .orElseGet(() -> findSameAccount(currentClusters, dh));
      restore(dh, existing, ClustersProperties.DataHubConfig::getAccessKey,
          ClustersProperties.DataHubConfig::setAccessKey);
      restore(dh, existing, ClustersProperties.DataHubConfig::getSecurityToken,
          ClustersProperties.DataHubConfig::setSecurityToken);
    }
    return submitted;
  }

  private static void restore(ClustersProperties.DataHubConfig submitted,
                              @Nullable ClustersProperties.DataHubConfig existing,
                              Function<ClustersProperties.DataHubConfig, String> getter,
                              BiConsumer<ClustersProperties.DataHubConfig, String> setter) {
    if (MASK.equals(getter.apply(submitted))) {
      setter.accept(submitted, existing == null ? null : getter.apply(existing));
    }
  }

  /** A renamed cluster is matched by the same endpoint and AccessKey ID. */
  @Nullable
  private static ClustersProperties.DataHubConfig findSameAccount(List<ClustersProperties.Cluster> clusters,
                                                                  ClustersProperties.DataHubConfig submitted) {
    if (submitted.getEndpoint() == null || submitted.getAccessId() == null) {
      return null;
    }
    return clusters.stream()
        .map(ClustersProperties.Cluster::getDatahub)
        .filter(dh -> submitted.getEndpoint().equals(dh.getEndpoint())
            && submitted.getAccessId().equals(dh.getAccessId()))
        .findFirst()
        .orElse(null);
  }

  @Nullable
  private static String maskValue(@Nullable String value) {
    return value == null || value.isEmpty() ? value : MASK;
  }
}
