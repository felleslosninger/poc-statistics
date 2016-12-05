package no.difi.statistics.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.*;


@XmlRootElement
public class TimeSeriesPoint implements Comparable<TimeSeriesPoint> {

    private ZonedDateTime timestamp;
    private List<Measurement> measurements = new ArrayList<>();

    private TimeSeriesPoint() {
        // Use builder
    }

    @XmlElement
    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    @XmlElement
    public List<Measurement> getMeasurements() {
        return measurements;
    }

    public Optional<Measurement> getMeasurement(String name) {
        return measurements.stream().filter(m -> m.getId().equals(name)).findFirst();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int compareTo(TimeSeriesPoint other) {
        return timestamp.compareTo(other.timestamp);
    }

    public static class Builder {
        private TimeSeriesPoint instance;
        private Map<String, Measurement> measurements = new HashMap<>();

        Builder() {
            this.instance = new TimeSeriesPoint();
        }

        public Builder timestamp(ZonedDateTime timestamp) {
            instance.timestamp = timestamp;
            return this;
        }

        public Builder measurement(String measurementId, long measurement) {
            measurement(new Measurement(measurementId, measurement));
            return this;
        }

        public Builder measurement(Measurement measurement) {
            Measurement oldMeasurement = measurements.get(measurement.getId());
            if (oldMeasurement != null)
                measurement = new Measurement(measurement.getId(), measurement.getValue() + oldMeasurement.getValue());
            measurements.put(measurement.getId(), measurement);
            return this;
        }

        public Builder measurements(List<Measurement> measurements) {
            measurements.forEach(this::measurement);
            return this;
        }

        public Builder plus(TimeSeriesPoint other) {
            measurements(other.measurements);
            return this;
        }

        public TimeSeriesPoint build() {
            if (instance.timestamp == null) throw new IllegalArgumentException("timestamp");
            instance.measurements.addAll(measurements.values());
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
            return TimeSeriesPoint.builder().timestamp(ZonedDateTime.parse(node.get("timestamp").asText())).measurement(node.get("measurement").get("id").asText(), node.get("measurement").get("value").asInt()).build();
        }

    }
    @Override
    public String toString() {
        return "TimeSeriesPoint{" +
                "timestamp=" + timestamp +
                ", measurements=" + measurements +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeSeriesPoint that = (TimeSeriesPoint) o;

        return timestamp.equals(that.timestamp) && measurements.equals(that.measurements);

    }

    @Override
    public int hashCode() {
        int result = timestamp.hashCode();
        result = 31 * result + measurements.hashCode();
        return result;
    }

}
