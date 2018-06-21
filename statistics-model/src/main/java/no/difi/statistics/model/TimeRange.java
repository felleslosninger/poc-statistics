package no.difi.statistics.model;

import java.time.ZonedDateTime;
import java.util.Objects;

public class TimeRange {

    private ZonedDateTime from;
    private ZonedDateTime to;

    public TimeRange(ZonedDateTime from, ZonedDateTime to) {
        this.from = from;
        this.to = to;
    }

    public ZonedDateTime from() {
        return from;
    }

    public ZonedDateTime to() {
        return to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeRange timeRange = (TimeRange) o;
        return Objects.equals(from, timeRange.from) &&
                Objects.equals(to, timeRange.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public String toString() {
        return "TimeRange{" +
                "from=" + from +
                ", to=" + to +
                '}';
    }
}

