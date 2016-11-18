package no.difi.statistics.model;

/**
 * Definition for a time series
 */
public class TimeSeriesDefinition {

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
    public interface DistanceEntry { OwnerEntry distance(MeasurementDistance distance); }
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
        return name.equals(that.name) && distance == that.distance && owner.equals(that.owner);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + distance.hashCode();
        result = 31 * result + owner.hashCode();
        return result;
    }
}
