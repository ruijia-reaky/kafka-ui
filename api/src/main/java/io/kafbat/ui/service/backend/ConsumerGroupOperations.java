package io.kafbat.ui.service.backend;

import io.kafbat.ui.model.ConsumerGroupOrderingDTO;
import io.kafbat.ui.model.ConsumerGroupStateDTO;
import io.kafbat.ui.model.InternalConsumerGroup;
import io.kafbat.ui.model.InternalTopicConsumerGroup;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.model.SortOrderDTO;
import io.kafbat.ui.service.ConsumerGroupService.ConsumerGroupsPage;
import java.util.List;
import java.util.OptionalInt;
import javax.annotation.Nullable;
import reactor.core.publisher.Mono;

public interface ConsumerGroupOperations {

  Mono<ConsumerGroupsPage> getConsumerGroups(KafkaCluster cluster,
                                             OptionalInt pageNum,
                                             OptionalInt perPage,
                                             @Nullable String search,
                                             Boolean fts,
                                             ConsumerGroupOrderingDTO orderBy,
                                             SortOrderDTO sortOrderDto,
                                             List<ConsumerGroupStateDTO> states);

  Mono<List<InternalTopicConsumerGroup>> getConsumerGroupsForTopic(KafkaCluster cluster, String topic);

  Mono<InternalConsumerGroup> getConsumerGroupDetail(KafkaCluster cluster, String consumerGroupId);

  Mono<Void> deleteConsumerGroupById(KafkaCluster cluster, String groupId);
}
