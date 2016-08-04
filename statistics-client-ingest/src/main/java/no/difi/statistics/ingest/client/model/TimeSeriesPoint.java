package no.difi.statistics.ingest.client.model;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TimeSeriesPoint {

    private ZonedDateTime timestamp;
    private final List<Measurement> measurements = new ArrayList<>();

    private TimeSeriesPoint() {
        // Use builder
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public List<Measurement> getMeasurements() {
        return measurements;
    }

    public Optional<Measurement> getMeasurement(String name) {
        return measurements.stream().filter(m -> m.getId().equals(name)).findFirst();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final TimeSeriesPoint instance;

        Builder() {
            this.instance = new TimeSeriesPoint();
        }

        public Builder timestamp(ZonedDateTime timestamp) {
            instance.timestamp = timestamp;
            return this;
        }

        public Builder measurement(String measurementId, int measurement) {
            instance.measurements.add(new Measurement(measurementId, measurement));
            return this;
        }

        public Builder measurement(Measurement measurement) {
            instance.measurements.add(measurement);
            return this;
        }

        public Builder measurements(List<Measurement> measurements) {
            instance.measurements.addAll(measurements);
            return this;
        }

        public TimeSeriesPoint build() {
            if (instance.timestamp == null) throw new IllegalArgumentException("timestamp");
            return instance;
        }

    }

    @Override
    public String toString() {
        return "TimeSeriesPoint{" +
                "timestamp=" + timestamp +
                ", measurements=" + measurements +
                '}';
    }
}
