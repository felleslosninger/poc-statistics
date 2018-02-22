package no.difi.statistics.model;

import static java.lang.String.format;

/**
 * Definition for a time series
 */
public class TimeSeriesDefinition implements Comparable<TimeSeriesDefinition> {

    private String name;
    private MeasurementDistance distance;
    private String owner;

    private TimeSeriesDefinition() {
        // Use builder
    }

    public String getOwner() {
        return owner;
    }

    public String getName() {
        return name;
    }

    public MeasurementDistance getDistance() {
        return distance;
    }

    public static NameEntry builder() {
        return new Builder();
    }

    public interface NameEntry { DistanceEntry name(String name); }

    public interface DistanceEntry {
        OwnerEntry distance(MeasurementDistance distance);
        OwnerEntry years();
        OwnerEntry months();
        OwnerEntry days();
        OwnerEntry hours();
        OwnerEntry minutes();
    }

    public interface OwnerEntry { TimeSeriesDefinition owner(String owner); }

    public static class Builder implements NameEntry, DistanceEntry, OwnerEntry {

        private TimeSeriesDefinition instance = new TimeSeriesDefinition();
        @Override
        public DistanceEntry name(String name) {
            instance.name = name;
            return this;
        }

        @Override
        public OwnerEntry distance(MeasurementDistance distance) {
            instance.distance = distance;
            return this;
        }

        @Override
        public OwnerEntry years() {
            instance.distance = MeasurementDistance.years;
            return this;
        }

        @Override
        public OwnerEntry months() {
            instance.distance = MeasurementDistance.months;
            return this;
        }

        @Override
        public OwnerEntry days() {
            instance.distance = MeasurementDistance.days;
            return this;
        }

        @Override
        public OwnerEntry hours() {
            instance.distance = MeasurementDistance.hours;
            return this;
        }

        @Override
        public OwnerEntry minutes() {
            instance.distance = MeasurementDistance.minutes;
            return this;
        }

        @Override
        public TimeSeriesDefinition owner(String owner) {
            instance.owner = owner;
            return instance;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeSeriesDefinition that = (TimeSeriesDefinition) o;

        if (!name.equals(that.name)) return false;
        if (distance != that.distance) return false;
        return owner.equals(that.owner);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + distance.hashCode();
        result = 31 * result + owner.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return format("%s@%s@%s", owner, name, distance);
    }

    @Override
    public int compareTo(TimeSeriesDefinition other) {
        return toString().compareTo(other.toString());
    }

}
