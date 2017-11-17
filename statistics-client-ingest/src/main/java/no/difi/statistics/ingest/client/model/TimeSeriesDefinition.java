package no.difi.statistics.ingest.client.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.joining;

/**
 * Definition for a time series
 */
public class TimeSeriesDefinition implements Comparable<TimeSeriesDefinition> {

    private String name;
    private MeasurementDistance distance;
    private Map<String, String> categories;

    private TimeSeriesDefinition() {
        // Use builder
    }

    public String getName() {
        return name;
    }

    public MeasurementDistance getDistance() {
        return distance;
    }

    public Optional<Map<String, String>> getCategories() {
        return categories == null ? Optional.empty() : Optional.of(unmodifiableMap(categories));
    }

    public static NameEntry builder() {
        return new Builder();
    }

    public interface NameEntry { CategoryOrDistanceEntry name(String name); }

    public interface CategoryOrDistanceEntry {
        TimeSeriesDefinition distance(MeasurementDistance distance);
        CategoryOrDistanceEntry category(String key, String value);
    }

    public static class Builder implements NameEntry, CategoryOrDistanceEntry {

        private TimeSeriesDefinition instance = new TimeSeriesDefinition();
        @Override
        public CategoryOrDistanceEntry name(String name) {
            instance.name = name;
            return this;
        }

        @Override
        public TimeSeriesDefinition distance(MeasurementDistance distance) {
            instance.distance = distance;
            return instance;
        }

        @Override
        public CategoryOrDistanceEntry category(String key, String value) {
            if (instance.categories == null) instance.categories = new HashMap<>();
            instance.categories.put(key, value);
            return this;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeSeriesDefinition that = (TimeSeriesDefinition) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (distance != that.distance) return false;
        return categories != null ? categories.equals(that.categories) : that.categories == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (distance != null ? distance.hashCode() : 0);
        result = 31 * result + (categories != null ? categories.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return format("%s:%s", name, distance) +
                (categories != null ? format("?%s", categoriesAsString()) : "");
    }

    private String categoriesAsString() {
        return categories.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).sorted().collect(joining("&"));
    }

    @Override
    public int compareTo(TimeSeriesDefinition other) {
        return toString().compareTo(other.toString());
    }

}
