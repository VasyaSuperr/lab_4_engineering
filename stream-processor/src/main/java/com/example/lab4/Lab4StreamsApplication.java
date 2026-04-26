package com.example.lab4;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

@SpringBootApplication
public class Lab4StreamsApplication {

    private static final String INPUT_TOPIC = getenv("INPUT_TOPIC", "Topic1");

    private static final String AVG_DURATION_TOPIC = getenv("AVG_DURATION_TOPIC", "daily-average-duration");
    private static final String TRIP_COUNT_TOPIC = getenv("TRIP_COUNT_TOPIC", "daily-trip-count");
    private static final String POPULAR_START_TOPIC = getenv("POPULAR_START_TOPIC", "daily-most-popular-start-station");
    private static final String TOP3_STATIONS_TOPIC = getenv("TOP3_STATIONS_TOPIC", "daily-top3-stations");

    public static void main(String[] args) {
        SpringApplication.run(Lab4StreamsApplication.class, args);
        startKafkaStreams();
    }

    private static void startKafkaStreams() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab4-divvy-streams-app-v2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);

        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        JsonSerde<TripEvent> tripSerde = new JsonSerde<>(TripEvent.class);
        JsonSerde<DurationStats> durationStatsSerde = new JsonSerde<>(DurationStats.class);
        JsonSerde<StationStats> stationStatsSerde = new JsonSerde<>(StationStats.class);

        JsonSerde mapSerde = new JsonSerde<>(Map.class);
        JsonSerde listSerde = new JsonSerde<>(List.class);

        TimeWindows dailyWindow = TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ZERO);

        KStream<String, TripEvent> trips = builder
                .stream(
                        INPUT_TOPIC,
                        Consumed.with(stringSerde, tripSerde)
                                .withTimestampExtractor(new TripTimestampExtractor())
                )
                .filter((key, trip) -> trip != null && trip.start_time != null && !trip.start_time.isBlank())
                .selectKey((key, trip) -> extractDate(trip.start_time));

        trips
                .groupByKey(Grouped.with(stringSerde, tripSerde))
                .windowedBy(dailyWindow)
                .aggregate(
                        DurationStats::new,
                        (date, trip, stats) -> stats.add(trip.tripduration),
                        Materialized.with(stringSerde, durationStatsSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, stats) -> KeyValue.pair(
                        windowedKey.key(),
                        Map.of(
                                "date", windowedKey.key(),
                                "average_trip_duration_seconds", stats.average(),
                                "trip_count", stats.count
                        )
                ))
                .to(AVG_DURATION_TOPIC, Produced.with(stringSerde, mapSerde));

        trips
                .groupByKey(Grouped.with(stringSerde, tripSerde))
                .windowedBy(dailyWindow)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key(),
                        Map.of(
                                "date", windowedKey.key(),
                                "trip_count", count
                        )
                ))
                .to(TRIP_COUNT_TOPIC, Produced.with(stringSerde, mapSerde));

        trips
                .groupByKey(Grouped.with(stringSerde, tripSerde))
                .windowedBy(dailyWindow)
                .aggregate(
                        StationStats::new,
                        (date, trip, stats) -> stats.addStartStation(trip.from_station_name),
                        Materialized.with(stringSerde, stationStatsSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, stats) -> KeyValue.pair(
                        windowedKey.key(),
                        stats.mostPopularStartStation(windowedKey.key())
                ))
                .to(POPULAR_START_TOPIC, Produced.with(stringSerde, mapSerde));

        trips
                .groupByKey(Grouped.with(stringSerde, tripSerde))
                .windowedBy(dailyWindow)
                .aggregate(
                        StationStats::new,
                        (date, trip, stats) -> stats.addBothStations(trip.from_station_name, trip.to_station_name),
                        Materialized.with(stringSerde, stationStatsSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, stats) -> KeyValue.pair(
                        windowedKey.key(),
                        stats.top3Stations(windowedKey.key())
                ))
                .to(TOP3_STATIONS_TOPIC, Produced.with(stringSerde, listSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();

        System.out.println("Kafka Streams processor started. Reading from: " + INPUT_TOPIC);
    }

    private static String extractDate(String startTime) {
        return LocalDateTime.parse(startTime.replace(" ", "T")).toLocalDate().toString();
    }

    private static long extractTimestampMillis(String startTime) {
        return LocalDateTime
                .parse(startTime.replace(" ", "T"))
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();
    }

    private static String getenv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    public static class TripTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            Object value = record.value();

            if (value instanceof TripEvent) {
                TripEvent trip = (TripEvent) value;

                if (trip.start_time != null && !trip.start_time.isBlank()) {
                    try {
                        return extractTimestampMillis(trip.start_time);
                    } catch (Exception ignored) {
                        return partitionTime;
                    }
                }
            }

            return partitionTime;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TripEvent {
        public String start_time;
        public double tripduration;
        public String from_station_name;
        public String to_station_name;
    }

    public static class DurationStats {
        public long count = 0;
        public double totalDuration = 0.0;

        public DurationStats add(double duration) {
            count++;
            totalDuration += duration;
            return this;
        }

        public double average() {
            return count == 0 ? 0.0 : totalDuration / count;
        }
    }

    public static class StationStats {
        public Map<String, Long> stations = new HashMap<>();

        public StationStats addStartStation(String station) {
            add(station);
            return this;
        }

        public StationStats addBothStations(String from, String to) {
            add(from);
            add(to);
            return this;
        }

        private void add(String station) {
            if (station != null && !station.isBlank()) {
                stations.merge(station, 1L, Long::sum);
            }
        }

        public Map<String, Object> mostPopularStartStation(String date) {
            return stations.entrySet()
                    .stream()
                    .max(Map.Entry.comparingByValue())
                    .map(e -> Map.<String, Object>of(
                            "date", date,
                            "station_name", e.getKey(),
                            "count", e.getValue()
                    ))
                    .orElse(Map.of(
                            "date", date,
                            "station_name", "",
                            "count", 0
                    ));
        }

        public List<Map<String, Object>> top3Stations(String date) {
            return stations.entrySet()
                    .stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(3)
                    .map(e -> Map.<String, Object>of(
                            "date", date,
                            "station_name", e.getKey(),
                            "count", e.getValue()
                    ))
                    .collect(Collectors.toList());
        }
    }

    public static class JsonSerde<T> implements Serde<T> {
        private final ObjectMapper mapper = new ObjectMapper();
        private final Class<T> type;

        public JsonSerde() {
            this.type = null;
        }

        public JsonSerde(Class<T> type) {
            this.type = type;
        }

        @Override
        public Serializer<T> serializer() {
            return (topic, data) -> {
                try {
                    return mapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
        }

        @Override
        public Deserializer<T> deserializer() {
            return (topic, data) -> {
                try {
                    if (data == null || type == null) return null;
                    return mapper.readValue(new String(data, StandardCharsets.UTF_8), type);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }
}