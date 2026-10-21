package io.kafbat.ui.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import io.kafbat.ui.config.ClustersProperties;
import io.kafbat.ui.model.ApplicationPropertyValidationDTO;
import io.kafbat.ui.model.KafkaCluster;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Component
public class DataHubClientFactory {

  private final Map<String, DatahubClient> clients = new ConcurrentHashMap<>();

  public DatahubClient get(KafkaCluster cluster) {
    return clients.computeIfAbsent(cluster.getName(), n -> create(cluster.getDatahub()));
  }

  public DatahubClient create(ClustersProperties.DataHubConfig config) {
    AliyunAccount account = StringUtils.hasText(config.getSecurityToken())
        ? new AliyunAccount(config.getAccessId(), config.getAccessKey(), config.getSecurityToken())
        : new AliyunAccount(config.getAccessId(), config.getAccessKey());
    // Binary (protobuf) protocol is disabled: the SDK is built against protobuf 2.x,
    // while the application classpath resolves protobuf 3.x.
    return DatahubClientBuilder.newBuilder()
        .setDatahubConfig(new DatahubConfig(config.getEndpoint(), account, false))
        .build();
  }

  public Mono<ApplicationPropertyValidationDTO> validate(ClustersProperties.DataHubConfig config) {
    return Mono.fromCallable(() -> {
      if (config == null || !StringUtils.hasText(config.getEndpoint()) || config.getProjects() == null
          || config.getProjects().isEmpty()) {
        return invalid("datahub.endpoint and datahub.projects are required");
      }
      DatahubClient client = create(config);
      for (String project : config.getProjects()) {
        client.getProject(project);
      }
      return new ApplicationPropertyValidationDTO().error(false);
    }).subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> Mono.just(invalid(DataHubErrors.describe(e))));
  }

  private static ApplicationPropertyValidationDTO invalid(String message) {
    return new ApplicationPropertyValidationDTO().error(true).errorMessage(message);
  }
}
