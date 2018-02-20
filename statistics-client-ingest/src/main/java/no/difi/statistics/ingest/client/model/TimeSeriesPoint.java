package no.difi.statistics.ingest.client.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.*;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.joining;


@XmlRootElement
public class TimeSeriesPoint {

    private ZonedDateTime timestamp;
    private Map<String, Long> measurements = new HashMap<>();
    private Map<String, String> categories;

    private TimeSeriesPoint() {
        // Use builder
    }

    @XmlElement
    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    @XmlElement
    public Map<String, Long> getMeasurements() {
        return measurements;
    }

    public Optional<Long> getMeasurement(String name) {
        return measurements.entrySet().stream().filter(e -> e.getKey().equals(name)).map(Map.Entry::getValue).findFirst();
    }

    @XmlElement
    public Optional<Map<String, String>> getCategories() {
        return categories == null ? Optional.empty() : Optional.of(unmodifiableMap(categories));
    }

    public static TimestampEntry timeSeriesPoint() {
        return new Builder();
    }

    public interface TimestampEntry {
        MeasurementEntry timestamp(ZonedDateTime timestamp);
    }

    public interface MeasurementEntry {
        MeasurementOrCategoryOrBuildEntry measurement(String measurementId, long measurement);
        MeasurementOrCategoryOrBuildEntry measurements(Map<String, Long> measurements);
    }

    public interface CategoryEntry {
        CategoryOrBuildEntry category(String key, String value);
    }

    public interface BuildEntry {
        TimeSeriesPoint build();
    }

    public interface MeasurementOrCategoryOrBuildEntry extends MeasurementEntry, CategoryEntry, BuildEntry {}

    public interface CategoryOrBuildEntry extends CategoryEntry, BuildEntry {}

    public static class Builder implements TimestampEntry, MeasurementEntry, MeasurementOrCategoryOrBuildEntry, CategoryOrBuildEntry {
        private TimeSeriesPoint instance;
        private Map<String, String> categories = new HashMap<>();

        Builder() {
            this.instance = new TimeSeriesPoint();
        }

        @Override
        public MeasurementEntry timestamp(ZonedDateTime timestamp) {
            instance.timestamp = timestamp;
            return this;
        }

        @Override
        public MeasurementOrCategoryOrBuildEntry measurement(String measurementId, long measurement) {
            instance.measurements.put(measurementId, measurement);
            return this;
        }

        @Override
        public MeasurementOrCategoryOrBuildEntry  measurements(Map<String, Long> measurements) {
            instance.measurements.putAll(measurements);
            return this;
        }

        @Override
        public CategoryOrBuildEntry category(String key, String value) {
            categories.put(key, value);
            return this;
        }

        @Override
        public TimeSeriesPoint build() {
            if (instance.timestamp == null) throw new IllegalArgumentException("timestamp");
            if (!categories.isEmpty()) {
                instance.categories = categories;
            }
            return instance;
        }

    }

    /**
     * Use custom deserializer to maintain immutability property
     */
    static class TimeSeriesPointJsonDeserializer extends JsonDeserializer<TimeSeriesPoint> {

        @Override
        public TimeSeriesPoint deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
            JsonNode node = parser.getCodec().readTree(parser);
            return TimeSeriesPoint.timeSeriesPoint().timestamp(ZonedDateTime.parse(node.get("timestamp").asText())).measurement(node.get("measurement").get("id").asText(), node.get("measurement").get("value").asInt()).build();
        }

    }
    @Override
    public String toString() {
        return "TimeSeriesPoint{" +
                "timestamp=" + timestamp +
                ", measurements=" + measurements +
                (categories != null ? format(", categories=%s", categoriesAsString()) : "") +
        '}';
    }

    private String categoriesAsString() {
        return categories.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).sorted().collect(joining("&"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeSeriesPoint that = (TimeSeriesPoint) o;

        if (!timestamp.equals(that.timestamp)) return false;
        if (!measurements.equals(that.measurements)) return false;
        return categories != null ? categories.equals(that.categories) : that.categories == null;
    }

    @Override
    public int hashCode() {
        int result = timestamp.hashCode();
        result = 31 * result + measurements.hashCode();
        result = 31 * result + (categories != null ? categories.hashCode() : 0);
        return result;
    }

}
