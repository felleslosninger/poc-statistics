package no.difi.statistics.model.query;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class QueryFilter {

    private Map<String, String> categories;
    private ZonedDateTime from;
    private ZonedDateTime to;

    private QueryFilter() {
        // Use builder
    }

    public ZonedDateTime from() {
        return from;
    }

    public ZonedDateTime to() {
        return to;
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

        public Builder from(ZonedDateTime from) {
            instance.from = from;
            return this;
        }

        public Builder to(ZonedDateTime to) {
            instance.to = to;
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

        if (categories != null ? !categories.equals(that.categories) : that.categories != null) return false;
        if (from != null ? !from.equals(that.from) : that.from != null) return false;
        return to != null ? to.equals(that.to) : that.to == null;
    }

    @Override
    public int hashCode() {
        int result = categories != null ? categories.hashCode() : 0;
        result = 31 * result + (from != null ? from.hashCode() : 0);
        result = 31 * result + (to != null ? to.hashCode() : 0);
        return result;
    }
}
