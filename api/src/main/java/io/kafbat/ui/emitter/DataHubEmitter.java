package io.kafbat.ui.emitter;

import io.kafbat.ui.model.ConsumerPosition;
import io.kafbat.ui.model.PollingModeDTO;
import io.kafbat.ui.model.TopicMessageDTO;
import io.kafbat.ui.model.TopicMessageEventDTO;
import io.kafbat.ui.serdes.ConsumerRecordDeserializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import reactor.core.publisher.FluxSink;

/**
 * Message browsing for DataHub topics with the same paging semantics as {@link ForwardEmitter},
 * {@link BackwardEmitter} and {@link TailingEmitter}. DataHub can only read forward, so backward
 * pages are read as forward windows that end at the requested offset.
 */
@Slf4j
public class DataHubEmitter extends AbstractEmitter {

  private static final int TAILING_BATCH = 100;

  private final DataHubRecordSource source;
  private final ConsumerPosition position;
  private final int messagesPerPage;
  private final PollingSettings pollingSettings;
  private final @Nullable Cursor.Tracking cursor;

  public DataHubEmitter(DataHubRecordSource source,
                        ConsumerPosition position,
                        int messagesPerPage,
                        ConsumerRecordDeserializer deserializer,
                        Predicate<TopicMessageDTO> filter,
                        PollingSettings pollingSettings,
                        @Nullable Cursor.Tracking cursor) {
    super(new MessagesProcessing(deserializer, filter, isForward(position.pollingMode()),
        position.pollingMode() == PollingModeDTO.TAILING ? null : messagesPerPage), pollingSettings);
    this.source = source;
    this.position = position;
    this.messagesPerPage = messagesPerPage;
    this.pollingSettings = pollingSettings;
    this.cursor = position.pollingMode() == PollingModeDTO.TAILING ? null : cursor;
  }

  private static boolean isForward(PollingModeDTO mode) {
    return switch (mode) {
      case EARLIEST, FROM_OFFSET, FROM_TIMESTAMP, TAILING -> true;
      case LATEST, TO_OFFSET, TO_TIMESTAMP -> false;
    };
  }

  @Override
  public void accept(FluxSink<TopicMessageEventDTO> sink) {
    try {
      sendPhase(sink, "Consumer created");
      if (position.pollingMode() == PollingModeDTO.TAILING) {
        tail(sink);
        sink.complete();
      } else if (isForward(position.pollingMode())) {
        readForward(sink);
      } else {
        readBackward(sink);
      }
    } catch (Exception e) {
      log.error("Error reading DataHub topic {}", source.topic(), e);
      sink.error(e);
    }
  }

  private void readForward(FluxSink<TopicMessageEventDTO> sink) {
    Map<Integer, Long> begin = source.beginOffsets();
    Map<Integer, Long> end = source.endOffsets();
    TreeMap<Integer, Long> from = new TreeMap<>();
    for (int p : targetPartitions(end)) {
      long start = switch (position.pollingMode()) {
        case FROM_OFFSET -> requestedOffset(p);
        case FROM_TIMESTAMP -> source.offsetForTimestamp(p, position.timestamp());
        default -> begin.get(p);
      };
      from.put(p, clamp(start, begin.get(p), end.get(p)));
    }
    if (cursor != null) {
      // tracked offsets are advanced by one when the next page cursor is registered
      cursor.initOffsets(toTopicPartitions(from, -1));
    }
    while (!sink.isCancelled() && !isSendLimitReached()) {
      TreeMap<Integer, long[]> range = new TreeMap<>();
      int perPartition = perPartition(from.size());
      from.forEach((p, f) -> {
        if (f < end.get(p)) {
          range.put(p, new long[] {f, Math.min(end.get(p), f + perPartition)});
        }
      });
      if (range.isEmpty()) {
        sendFinishStatsAndCompleteSink(sink, null);
        return;
      }
      send(sink, poll(sink, range), cursor);
      range.forEach((p, r) -> from.put(p, r[1]));
    }
    boolean hasMore = isSendLimitReached()
        || from.entrySet().stream().anyMatch(e -> e.getValue() < end.get(e.getKey()));
    sendFinishStatsAndCompleteSink(sink, hasMore ? cursor : null);
  }

  private void readBackward(FluxSink<TopicMessageEventDTO> sink) {
    Map<Integer, Long> begin = source.beginOffsets();
    Map<Integer, Long> end = source.endOffsets();
    TreeMap<Integer, Long> to = new TreeMap<>();
    for (int p : targetPartitions(end)) {
      long limit = switch (position.pollingMode()) {
        case TO_OFFSET -> requestedOffset(p);
        case TO_TIMESTAMP -> source.offsetForTimestamp(p, position.timestamp());
        default -> end.get(p);
      };
      to.put(p, clamp(limit, begin.get(p), end.get(p)));
    }
    if (cursor != null) {
      cursor.initOffsets(toTopicPartitions(to, 0));
    }
    while (!sink.isCancelled() && !isSendLimitReached()) {
      TreeMap<Integer, long[]> range = new TreeMap<>();
      int perPartition = perPartition(to.size());
      to.forEach((p, t) -> {
        if (t > begin.get(p)) {
          range.put(p, new long[] {Math.max(begin.get(p), t - perPartition), t});
        }
      });
      if (range.isEmpty()) {
        sendFinishStatsAndCompleteSink(sink, null);
        return;
      }
      send(sink, poll(sink, range), cursor);
      range.forEach((p, r) -> to.put(p, r[0]));
    }
    boolean hasMore = isSendLimitReached()
        || to.entrySet().stream().anyMatch(e -> e.getValue() > begin.get(e.getKey()));
    sendFinishStatsAndCompleteSink(sink, hasMore ? cursor : null);
  }

  private void tail(FluxSink<TopicMessageEventDTO> sink) throws InterruptedException {
    Map<Integer, Long> next = new HashMap<>();
    for (int p : targetPartitions(source.endOffsets())) {
      next.put(p, source.endOffsets().get(p));
    }
    while (!sink.isCancelled()) {
      sendPhase(sink, "Polling");
      TreeMap<Integer, long[]> range = new TreeMap<>();
      next.forEach((p, n) -> range.put(p, new long[] {n, Long.MAX_VALUE}));
      List<ConsumerRecord<Bytes, Bytes>> polled = poll(sink, range, TAILING_BATCH);
      polled.forEach(r -> next.merge(r.partition(), r.offset() + 1, Math::max));
      send(sink, polled, null);
      if (polled.isEmpty()) {
        Thread.sleep(pollingSettings.getPollTimeout().toMillis());
      }
    }
  }

  private List<ConsumerRecord<Bytes, Bytes>> poll(FluxSink<TopicMessageEventDTO> sink, TreeMap<Integer, long[]> range) {
    return poll(sink, range, Integer.MAX_VALUE);
  }

  private List<ConsumerRecord<Bytes, Bytes>> poll(FluxSink<TopicMessageEventDTO> sink,
                                                  TreeMap<Integer, long[]> range,
                                                  int maxPerPartition) {
    long started = System.nanoTime();
    Map<TopicPartition, List<ConsumerRecord<Bytes, Bytes>>> polled = new HashMap<>();
    List<ConsumerRecord<Bytes, Bytes>> result = new ArrayList<>();
    range.forEach((p, r) -> {
      int max = (int) Math.min(maxPerPartition, r[1] - r[0]);
      List<ConsumerRecord<Bytes, Bytes>> records = source.read(p, r[0], r[1], max);
      if (!records.isEmpty()) {
        polled.put(new TopicPartition(source.topic(), p), records);
        result.addAll(records);
      }
    });
    sendConsuming(sink, PolledRecords.create(new ConsumerRecords<>(polled),
        Duration.ofNanos(System.nanoTime() - started)));
    return result;
  }

  private List<Integer> targetPartitions(Map<Integer, Long> available) {
    List<Integer> requested = position.partitions().stream().map(TopicPartition::partition).toList();
    return available.keySet().stream()
        .filter(p -> requested.isEmpty() || requested.contains(p))
        .sorted()
        .toList();
  }

  private long requestedOffset(int partition) {
    var offsets = position.offsets();
    if (offsets == null) {
      throw new IllegalStateException("offsets not provided for " + position.pollingMode());
    }
    if (offsets.tpOffsets() != null) {
      Long offset = offsets.tpOffsets().get(new TopicPartition(position.topic(), partition));
      return offset == null ? source.beginOffsets().get(partition) : offset;
    }
    return offsets.offset();
  }

  private int perPartition(int partitions) {
    return Math.max(1, (int) Math.ceil((double) messagesPerPage / Math.max(1, partitions)));
  }

  private Map<TopicPartition, Long> toTopicPartitions(Map<Integer, Long> offsets, long delta) {
    return offsets.entrySet().stream()
        .collect(Collectors.toMap(e -> new TopicPartition(source.topic(), e.getKey()), e -> e.getValue() + delta));
  }

  private static long clamp(long value, long min, long max) {
    return Math.max(min, Math.min(max, value));
  }
}
