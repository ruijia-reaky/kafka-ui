package io.kafbat.ui.emitter;

import static org.assertj.core.api.Assertions.assertThat;

import io.kafbat.ui.model.ConsumerPosition;
import io.kafbat.ui.model.PollingModeDTO;
import io.kafbat.ui.model.TopicMessageDTO;
import io.kafbat.ui.model.TopicMessageEventDTO;
import io.kafbat.ui.serde.api.Serde;
import io.kafbat.ui.serdes.ConsumerRecordDeserializer;
import io.kafbat.ui.serdes.PropertyResolverImpl;
import io.kafbat.ui.serdes.builtin.StringSerde;
import io.kafbat.ui.service.PollingCursorsStorage;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

class DataHubEmitterTest {

  private static final String TOPIC = "dh_topic";
  private static final int PAGE_SIZE = 11;

  private final PollingCursorsStorage cursorsStorage = new PollingCursorsStorage();

  @Test
  void forwardPagingWithCursor() {
    var source = new InMemorySource(Map.of(0, 20));
    var position = new ConsumerPosition(PollingModeDTO.EARLIEST, TOPIC, List.of(), null, null);

    assertThat(offsets(emit(source, position, PAGE_SIZE))).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    Cursor cursor = singleCursor();
    assertThat(cursor.consumerPosition().pollingMode()).isEqualTo(PollingModeDTO.FROM_OFFSET);
    assertThat(cursor.consumerPosition().offsets().tpOffsets()).containsEntry(new TopicPartition(TOPIC, 0), 11L);

    assertThat(offsets(emit(source, cursor.consumerPosition(), PAGE_SIZE)))
        .containsExactly(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L);
    assertThat(cursorsStorage.asMap()).hasSize(1);
  }

  @Test
  void backwardPagingWithCursor() {
    var source = new InMemorySource(Map.of(0, 20));
    var position = new ConsumerPosition(PollingModeDTO.LATEST, TOPIC, List.of(), null, null);

    assertThat(offsets(emit(source, position, PAGE_SIZE)))
        .containsExactly(19L, 18L, 17L, 16L, 15L, 14L, 13L, 12L, 11L, 10L, 9L);
    Cursor cursor = singleCursor();
    assertThat(cursor.consumerPosition().pollingMode()).isEqualTo(PollingModeDTO.TO_OFFSET);
    assertThat(cursor.consumerPosition().offsets().tpOffsets()).containsEntry(new TopicPartition(TOPIC, 0), 9L);

    assertThat(offsets(emit(source, cursor.consumerPosition(), PAGE_SIZE)))
        .containsExactly(8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L);
    assertThat(cursorsStorage.asMap()).hasSize(1);
  }

  @Test
  void fullyReadTopicDoesNotRegisterCursor() {
    var source = new InMemorySource(Map.of(0, 5, 1, 3));
    var position = new ConsumerPosition(PollingModeDTO.EARLIEST, TOPIC, List.of(), null, null);

    assertThat(emit(source, position, 100)).hasSize(8);
    assertThat(cursorsStorage.asMap()).isEmpty();
  }

  @Test
  void startsFromOldestAvailableSequence() {
    var source = new InMemorySource(Map.of(0, 10));
    source.begin.put(0, 4L);
    var position = new ConsumerPosition(PollingModeDTO.EARLIEST, TOPIC, List.of(), null, null);

    assertThat(offsets(emit(source, position, 100))).containsExactly(4L, 5L, 6L, 7L, 8L, 9L);
  }

  @Test
  void fromAndToTimestamp() {
    var source = new InMemorySource(Map.of(0, 10));
    long ts = InMemorySource.timestamp(6);

    var from = new ConsumerPosition(PollingModeDTO.FROM_TIMESTAMP, TOPIC, List.of(), ts, null);
    assertThat(offsets(emit(source, from, 100))).containsExactly(6L, 7L, 8L, 9L);

    var to = new ConsumerPosition(PollingModeDTO.TO_TIMESTAMP, TOPIC, List.of(), ts, null);
    assertThat(offsets(emit(source, to, 100))).containsExactly(5L, 4L, 3L, 2L, 1L, 0L);
  }

  @Test
  void fromAndToOffsetForSelectedPartition() {
    var source = new InMemorySource(Map.of(0, 10, 1, 10));
    var tp1 = new TopicPartition(TOPIC, 1);

    var from = new ConsumerPosition(PollingModeDTO.FROM_OFFSET, TOPIC, List.of(tp1), null,
        new ConsumerPosition.Offsets(7L, null));
    List<TopicMessageDTO> forward = emit(source, from, 100);
    assertThat(offsets(forward)).containsExactly(7L, 8L, 9L);
    assertThat(forward).allMatch(m -> m.getPartition() == 1);

    var to = new ConsumerPosition(PollingModeDTO.TO_OFFSET, TOPIC, List.of(), null,
        new ConsumerPosition.Offsets(null, Map.of(tp1, 2L)));
    assertThat(offsets(emit(source, to, 100))).containsExactly(1L, 0L);
  }

  @Test
  void offsetsOutOfRangeAreClamped() {
    var source = new InMemorySource(Map.of(0, 5));
    var from = new ConsumerPosition(PollingModeDTO.FROM_OFFSET, TOPIC, List.of(), null,
        new ConsumerPosition.Offsets(100L, null));
    assertThat(emit(source, from, 100)).isEmpty();

    var to = new ConsumerPosition(PollingModeDTO.TO_OFFSET, TOPIC, List.of(), null,
        new ConsumerPosition.Offsets(-5L, null));
    assertThat(emit(source, to, 100)).isEmpty();
  }

  @Test
  void tailingEmitsOnlyNewRecords() {
    var source = new InMemorySource(Map.of(0, 3));
    var position = new ConsumerPosition(PollingModeDTO.TAILING, TOPIC, List.of(), null, null);
    var emitter = new DataHubEmitter(source, position, PAGE_SIZE, deserializer(), m -> true,
        PollingSettings.createDefault(), null);

    var received = Flux.create(emitter)
        .subscribeOn(Schedulers.boundedElastic())
        .filter(e -> e.getType() == TopicMessageEventDTO.TypeEnum.MESSAGE)
        .map(TopicMessageEventDTO::getMessage)
        .doOnSubscribe(s -> Schedulers.boundedElastic().schedule(() -> source.append(0, 2), 300,
            java.util.concurrent.TimeUnit.MILLISECONDS))
        .take(2)
        .collectList()
        .block(Duration.ofSeconds(10));

    assertThat(offsets(Objects.requireNonNull(received))).containsExactly(3L, 4L);
  }

  private List<TopicMessageDTO> emit(DataHubRecordSource source, ConsumerPosition position, int limit) {
    var emitter = new DataHubEmitter(source, position, limit, deserializer(), m -> true,
        PollingSettings.createDefault(),
        cursorsStorage.createNewCursor(deserializer(), position, m -> true, limit));
    List<TopicMessageEventDTO> events = Flux.create(emitter).collectList().block();
    assertThat(events).isNotNull();
    assertThat(events.getLast().getType()).isEqualTo(TopicMessageEventDTO.TypeEnum.DONE);
    return events.stream()
        .filter(e -> e.getType() == TopicMessageEventDTO.TypeEnum.MESSAGE)
        .map(TopicMessageEventDTO::getMessage)
        .toList();
  }

  private Cursor singleCursor() {
    assertThat(cursorsStorage.asMap()).hasSize(1);
    return cursorsStorage.asMap().values().iterator().next();
  }

  private static List<Long> offsets(List<TopicMessageDTO> messages) {
    return messages.stream().map(TopicMessageDTO::getOffset).toList();
  }

  private static ConsumerRecordDeserializer deserializer() {
    Serde s = new StringSerde();
    s.configure(PropertyResolverImpl.empty(), PropertyResolverImpl.empty(), PropertyResolverImpl.empty());
    return new ConsumerRecordDeserializer(
        StringSerde.NAME,
        s.deserializer(null, Serde.Target.KEY),
        StringSerde.NAME,
        s.deserializer(null, Serde.Target.VALUE),
        StringSerde.NAME,
        s.deserializer(null, Serde.Target.KEY),
        s.deserializer(null, Serde.Target.VALUE),
        msg -> msg
    );
  }

  private static class InMemorySource implements DataHubRecordSource {

    final Map<Integer, Long> begin = new TreeMap<>();
    final Map<Integer, List<ConsumerRecord<Bytes, Bytes>>> records = new TreeMap<>();

    InMemorySource(Map<Integer, Integer> counts) {
      counts.forEach((p, cnt) -> {
        begin.put(p, 0L);
        records.put(p, new CopyOnWriteArrayList<>());
        append(p, cnt);
      });
    }

    static long timestamp(long offset) {
      return 1_000_000L + offset * 10;
    }

    void append(int partition, int count) {
      var list = records.get(partition);
      for (int i = 0; i < count; i++) {
        long offset = list.size();
        byte[] value = ("msg_" + offset).getBytes(StandardCharsets.UTF_8);
        list.add(new ConsumerRecord<>(TOPIC, partition, offset, timestamp(offset), TimestampType.LOG_APPEND_TIME,
            -1, value.length, null, new Bytes(value), new RecordHeaders(), java.util.Optional.empty()));
      }
    }

    @Override
    public String topic() {
      return TOPIC;
    }

    @Override
    public Map<Integer, Long> beginOffsets() {
      return begin;
    }

    @Override
    public Map<Integer, Long> endOffsets() {
      Map<Integer, Long> end = new TreeMap<>();
      records.forEach((p, l) -> end.put(p, (long) l.size()));
      return end;
    }

    @Override
    public List<ConsumerRecord<Bytes, Bytes>> read(int partition, long from, long to, int maxCount) {
      List<ConsumerRecord<Bytes, Bytes>> result = new ArrayList<>();
      for (var r : records.get(partition)) {
        if (r.offset() >= Math.max(from, begin.get(partition)) && r.offset() < to && result.size() < maxCount) {
          result.add(r);
        }
      }
      return result;
    }

    @Override
    public long offsetForTimestamp(int partition, long timestamp) {
      return records.get(partition).stream()
          .filter(r -> r.timestamp() >= timestamp)
          .map(ConsumerRecord::offset)
          .findFirst()
          .orElse((long) records.get(partition).size());
    }
  }
}
