package io.kafbat.ui.datahub;

import io.kafbat.ui.exception.TopicNotFoundException;
import io.kafbat.ui.model.KafkaCluster;
import java.util.List;

/**
 * Topic name as shown in UI. With a single configured project it is the plain DataHub topic name,
 * with several projects it is prefixed: {@code project.topic} (DataHub names can't contain dots).
 */
public record DataHubTopicName(String project, String topic) {

  public static DataHubTopicName parse(KafkaCluster cluster, String uiName) {
    List<String> projects = cluster.getDatahub().getProjects();
    if (projects.size() == 1) {
      return new DataHubTopicName(projects.getFirst(), uiName);
    }
    int dot = uiName.indexOf('.');
    if (dot <= 0 || !projects.contains(uiName.substring(0, dot))) {
      throw new TopicNotFoundException();
    }
    return new DataHubTopicName(uiName.substring(0, dot), uiName.substring(dot + 1));
  }

  public static String format(KafkaCluster cluster, String project, String topic) {
    return cluster.getDatahub().getProjects().size() == 1 ? topic : project + "." + topic;
  }

  public String format(KafkaCluster cluster) {
    return format(cluster, project, topic);
  }
}
