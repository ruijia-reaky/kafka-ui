package io.kafbat.ui.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ClusterTypeValidationTest {

  private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

  @Test
  void kafkaClusterRequiresBootstrapServers() {
    var cluster = new ClustersProperties.Cluster();
    cluster.setName("kafka");

    Set<ConstraintViolation<ClustersProperties.Cluster>> violations = validator.validate(cluster);

    assertThat(violations).extracting(ConstraintViolation::getMessage)
        .containsExactly("field bootstrapServers for for cluster could not be blank");
  }

  @Test
  void dataHubClusterNeedsNoBootstrapServersButDataHubSettings() {
    var cluster = new ClustersProperties.Cluster();
    cluster.setName("dh");
    cluster.setType(ClustersProperties.ClusterType.DATAHUB);
    assertThat(validator.validate(cluster)).isEmpty();

    var properties = new ClustersProperties();
    properties.setClusters(List.of(cluster));
    assertThatThrownBy(properties::validateAndSetDefaults)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("datahub.endpoint");
  }
}
