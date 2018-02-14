package no.difi.statistics.ingest.client.model;

import static java.lang.String.format;

/**
 * Definition for a time series
 */
public class TimeSeriesDefinition implements Comparable<TimeSeriesDefinition> {

    private String name;
    private MeasurementDistance distance;

    private TimeSeriesDefinition() {
        // Use builder
    }

    public String getName() {
        return name;
    }

    public MeasurementDistance getDistance() {
        return distance;
    }

    public static NameEntry timeSeriesDefinition() {
        return new Builder();
    }

    public interface NameEntry { DistanceEntry name(String name); }

    public interface DistanceEntry {
        TimeSeriesDefinition distance(MeasurementDistance distance);
    }

    public static class Builder implements NameEntry, DistanceEntry {

        private TimeSeriesDefinition instance = new TimeSeriesDefinition();
        @Override
        public DistanceEntry name(String name) {
            instance.name = name;
            return this;
        }

        @Override
        public TimeSeriesDefinition distance(MeasurementDistance distance) {
            instance.distance = distance;
            return instance;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeSeriesDefinition that = (TimeSeriesDefinition) o;
        return name.equals(that.name) && distance == that.distance;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + distance.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return format("%s:%s", name, distance);
    }

    @Override
    public int compareTo(TimeSeriesDefinition other) {
        return toString().compareTo(other.toString());
    }

}
