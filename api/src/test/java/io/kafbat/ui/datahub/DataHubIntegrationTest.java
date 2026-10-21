package io.kafbat.ui.datahub;

import static org.assertj.core.api.Assertions.assertThat;

import io.kafbat.ui.model.ClusterDTO;
import io.kafbat.ui.model.CreateTopicMessageDTO;
import io.kafbat.ui.model.DataHubShardDTO;
import io.kafbat.ui.model.ServerStatusDTO;
import io.kafbat.ui.model.TopicCreationDTO;
import io.kafbat.ui.model.TopicMessageEventDTO;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Runs against a real DataHub project, enabled only when DATAHUB_IT_ENDPOINT, DATAHUB_IT_PROJECT,
 * DATAHUB_IT_ACCESS_ID and DATAHUB_IT_ACCESS_KEY are set. Creates and deletes its own topic.
 */
@Tag("datahub")
@EnabledIfEnvironmentVariable(named = "DATAHUB_IT_ACCESS_KEY", matches = ".+")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "120000")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataHubIntegrationTest {

  private static final String CLUSTER = "datahub-it";
  private static final String TOPIC = "kafbat_it_" + System.currentTimeMillis();

  @Autowired
  private WebTestClient webTestClient;

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("kafka.clusters.0.name", () -> CLUSTER);
    registry.add("kafka.clusters.0.type", () -> "DATAHUB");
    registry.add("kafka.clusters.0.datahub.endpoint", () -> System.getenv("DATAHUB_IT_ENDPOINT"));
    registry.add("kafka.clusters.0.datahub.projects.0", () -> System.getenv("DATAHUB_IT_PROJECT"));
    registry.add("kafka.clusters.0.datahub.accessId", () -> System.getenv("DATAHUB_IT_ACCESS_ID"));
    registry.add("kafka.clusters.0.datahub.accessKey", () -> System.getenv("DATAHUB_IT_ACCESS_KEY"));
    registry.add("dynamic.config.enabled", () -> "false");
  }

  @BeforeAll
  void clusterIsOnlineAndTopicCreated() throws InterruptedException {
    ClusterDTO cluster = null;
    for (int i = 0; i < 60 && (cluster == null || cluster.getStatus() != ServerStatusDTO.ONLINE); i++) {
      Thread.sleep(1000);
      cluster = webTestClient.get().uri("/api/clusters").exchange()
          .expectStatus().isOk()
          .expectBodyList(ClusterDTO.class).returnResult().getResponseBody()
          .stream().filter(c -> CLUSTER.equals(c.getName())).findFirst().orElse(null);
    }
    assertThat(cluster).isNotNull();
    assertThat(cluster.getStatus()).isEqualTo(ServerStatusDTO.ONLINE);
    assertThat(cluster.getFeatures()).contains(ClusterDTO.FeaturesEnum.DATAHUB_SHARDS)
        .doesNotContain(ClusterDTO.FeaturesEnum.BROKERS_VIEW);

    webTestClient.post().uri("/api/clusters/{c}/topics", CLUSTER)
        .bodyValue(new TopicCreationDTO().name(TOPIC).partitions(2).configs(Map.of(
            "retention.ms", "86400000",
            "datahub.record.type", "TUPLE",
            "datahub.schema", "id:STRING,value:DOUBLE?")))
        .exchange()
        .expectStatus().isOk();
  }

  @AfterAll
  void deleteTopic() {
    webTestClient.delete().uri("/api/clusters/{c}/topics/{t}", CLUSTER, TOPIC).exchange();
  }

  @Test
  void sendAndReadTupleMessages() {
    for (int i = 0; i < 3; i++) {
      webTestClient.post().uri("/api/clusters/{c}/topics/{t}/messages", CLUSTER, TOPIC)
          .bodyValue(new CreateTopicMessageDTO().partition(i % 2).keySerde("String").valueSerde("String")
              .value("{\"id\":\"m" + i + "\",\"value\":" + i + ".5}"))
          .exchange()
          .expectStatus().isOk();
    }
    webTestClient.post().uri("/api/clusters/{c}/topics/{t}/messages", CLUSTER, TOPIC)
        .bodyValue(new CreateTopicMessageDTO().partition(0).keySerde("String").valueSerde("String")
            .value("{\"unknown\":1}"))
        .exchange()
        .expectStatus().isBadRequest();

    List<TopicMessageEventDTO> events = webTestClient.get()
        .uri("/api/clusters/{c}/topics/{t}/messages/v2?mode=EARLIEST&limit=10&keySerde=String&valueSerde=String",
            CLUSTER, TOPIC)
        .accept(MediaType.TEXT_EVENT_STREAM)
        .exchange()
        .expectStatus().isOk()
        .returnResult(TopicMessageEventDTO.class)
        .getResponseBody()
        .collectList()
        .block(Duration.ofMinutes(1));

    List<String> values = Objects.requireNonNull(events).stream()
        .filter(e -> e.getType() == TopicMessageEventDTO.TypeEnum.MESSAGE)
        .map(e -> e.getMessage().getValue())
        .sorted()
        .toList();
    assertThat(values).containsExactly(
        "{\"id\":\"m0\",\"value\":0.5}", "{\"id\":\"m1\",\"value\":1.5}", "{\"id\":\"m2\",\"value\":2.5}");
  }

  @Test
  void shardsAreListed() {
    List<DataHubShardDTO> shards = webTestClient.get()
        .uri("/api/clusters/{c}/topics/{t}/datahub/shards", CLUSTER, TOPIC)
        .exchange()
        .expectStatus().isOk()
        .expectBody(new ParameterizedTypeReference<List<DataHubShardDTO>>() {
        })
        .returnResult().getResponseBody();
    assertThat(shards).extracting(DataHubShardDTO::getShardId).containsExactly("0", "1");
  }
}
