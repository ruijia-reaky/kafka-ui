package io.kafbat.ui.datahub;

import static org.assertj.core.api.Assertions.assertThat;

import io.kafbat.ui.config.ClustersProperties;
import io.kafbat.ui.model.ApplicationConfigPropertiesDTO;
import io.kafbat.ui.model.ApplicationConfigPropertiesKafkaClustersInnerDTO;
import io.kafbat.ui.model.ApplicationConfigPropertiesKafkaClustersInnerDatahubDTO;
import io.kafbat.ui.model.ApplicationConfigPropertiesKafkaDTO;
import io.kafbat.ui.util.DynamicConfigOperations;
import java.util.List;
import org.junit.jupiter.api.Test;

class DataHubConfigSecretsTest {

  @Test
  void secretsAreMaskedInApiOutput() {
    var datahub = new ApplicationConfigPropertiesKafkaClustersInnerDatahubDTO()
        .endpoint("https://dh.example.com")
        .accessId("id")
        .accessKey("secret")
        .securityToken("");
    var dto = new ApplicationConfigPropertiesDTO()
        .kafka(new ApplicationConfigPropertiesKafkaDTO()
            .clusters(List.of(new ApplicationConfigPropertiesKafkaClustersInnerDTO().name("dh").datahub(datahub))));

    DataHubConfigSecrets.mask(dto);

    assertThat(datahub.getAccessKey()).isEqualTo(DataHubConfigSecrets.MASK);
    assertThat(datahub.getSecurityToken()).isEmpty();
    assertThat(datahub.getAccessId()).isEqualTo("id");
  }

  @Test
  void maskedSecretsAreRestoredFromCurrentConfig() {
    var current = properties(cluster("dh", "real-key", "real-token"));
    var submitted = properties(cluster("dh", DataHubConfigSecrets.MASK, "new-token"),
        cluster("other", DataHubConfigSecrets.MASK, null));

    DataHubConfigSecrets.restore(submitted, current);

    var restored = submitted.getKafka().getClusters();
    assertThat(restored.get(0).getDatahub().getAccessKey()).isEqualTo("real-key");
    assertThat(restored.get(0).getDatahub().getSecurityToken()).isEqualTo("new-token");
    assertThat(restored.get(1).getDatahub().getAccessKey()).isNull();
  }

  private static DynamicConfigOperations.PropertiesStructure properties(ClustersProperties.Cluster... clusters) {
    var kafka = new ClustersProperties();
    kafka.setClusters(List.of(clusters));
    return DynamicConfigOperations.PropertiesStructure.builder().kafka(kafka).build();
  }

  private static ClustersProperties.Cluster cluster(String name, String accessKey, String token) {
    var dh = new ClustersProperties.DataHubConfig();
    dh.setAccessKey(accessKey);
    dh.setSecurityToken(token);
    var cluster = new ClustersProperties.Cluster();
    cluster.setName(name);
    cluster.setType(ClustersProperties.ClusterType.DATAHUB);
    cluster.setDatahub(dh);
    return cluster;
  }
}
