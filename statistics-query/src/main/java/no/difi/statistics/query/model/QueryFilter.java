package no.difi.statistics.query.model;

import no.difi.statistics.model.TimeRange;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class QueryFilter {

    private Map<String, String> categories;
    private TimeRange timeRange;

    private QueryFilter() {
        // Use builder
    }

    public TimeRange timeRange() {
        return timeRange;
    }

    public Map<String, String> categories() {
        if (categories == null)
            return emptyMap();
        return unmodifiableMap(categories);
    }

    public static Builder queryFilter() {
        return new Builder(new QueryFilter());
    }

    public static class Builder {

        private final QueryFilter instance;

        Builder(QueryFilter instance) {
            this.instance = instance;
        }

        public Builder categories(Map<String, String> categories) {
            if (categories == null) return this;
            if (instance.categories == null) instance.categories = new HashMap<>();
            instance.categories.putAll(categories);
            return this;
        }

        public Builder categories(String categories) {
            if (categories == null) return this;
            if (instance.categories == null) instance.categories = new HashMap<>();
            stream(categories.split(",")).map(kv -> kv.split("=")).forEach(kv -> instance.categories.put(kv[0], kv[1]));
            return this;
        }

        public Builder range(ZonedDateTime from, ZonedDateTime to) {
            if (from != null || to != null)
                instance.timeRange = new TimeRange(from, to);
            return this;
        }

        public QueryFilter build() {
            return instance;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryFilter that = (QueryFilter) o;
        return Objects.equals(categories, that.categories) &&
                Objects.equals(timeRange, that.timeRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(categories, timeRange);
    }

    @Override
    public String toString() {
        return "QueryFilter{" +
                "categories=" + categories +
                ", timeRange=" + timeRange +
                '}';
    }

}
