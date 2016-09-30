package no.difi.statistics.elasticsearch;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.*;
import static java.util.Collections.singletonList;

public class IndexNameResolver {

    private String seriesName;
    private String owner;
    private ChronoUnit measurementDistance;
    private ChronoUnit baseTimeUnit;
    private ZonedDateTime from;
    private ZonedDateTime to;
    private ZonedDateTime at;

    public static SeriesName resolveIndexName() {
        return new Fluent();
    }

    public interface From {
        To from(ZonedDateTime from);
    }

    public interface To {
        ResolveList to(ZonedDateTime to);
    }

    public interface FromOrAtOrResolveList extends From, At, ResolveList {
    }

    public interface At {
        ResolveSingle at(ZonedDateTime at);
    }

    public interface ResolveList {
        List<String> list();
    }

    public interface ResolveSingle {
        String single();
    }

    public interface SeriesName {
        Owner seriesName(String seriesName);
    }

    public interface Owner {
        Distance owner(String owner);
    }

    public interface Distance {
        FromOrAtOrResolveList minutes();
        FromOrAtOrResolveList hours();
        FromOrAtOrResolveList days();
        FromOrAtOrResolveList months();
        FromOrAtOrResolveList years();
    }

    private static class Fluent implements
            From,
            To,
            FromOrAtOrResolveList,
            At,
            ResolveList,
            ResolveSingle,
            Owner,
            SeriesName,
            Distance {

        private IndexNameResolver instance = new IndexNameResolver();

        @Override
        public To from(ZonedDateTime from) {
            instance.from = from;
            return this;
        }

        @Override
        public ResolveList to(ZonedDateTime to) {
            instance.to = to;
            return this;
        }

        @Override
        public ResolveSingle at(ZonedDateTime at) {
            instance.at = at;
            return this;
        }

        @Override
        public Owner seriesName(String seriesName) {
            instance.seriesName = seriesName;
            return this;
        }

        @Override
        public Distance owner(String organizationNumber) {
            instance.owner = organizationNumber;
            return this;
        }

        @Override
        public FromOrAtOrResolveList minutes() {
            instance.measurementDistance = MINUTES;
            instance.baseTimeUnit = DAYS;
            return this;
        }

        @Override
        public FromOrAtOrResolveList hours() {
            instance.measurementDistance = HOURS;
            instance.baseTimeUnit = DAYS;
            return this;
        }

        @Override
        public FromOrAtOrResolveList days() {
            instance.measurementDistance = DAYS;
            instance.baseTimeUnit = YEARS;
            return this;
        }

        @Override
        public FromOrAtOrResolveList months() {
            instance.measurementDistance = MONTHS;
            instance.baseTimeUnit = YEARS;
            return this;
        }

        @Override
        public FromOrAtOrResolveList years() {
            instance.measurementDistance = YEARS;
            instance.baseTimeUnit = FOREVER;
            return this;
        }

        @Override
        public List<String> list() {
            if (instance.baseTimeUnit == FOREVER)
                return singletonList(formatName(instance.from));
            List<String> indices = new ArrayList<>();
            if (instance.from == null && instance.to == null) {
                indices.add(formatName(null));
            } else {
                instance.from = truncate(instance.from, instance.baseTimeUnit);
                instance.to = truncate(instance.to, instance.baseTimeUnit);
                for (ZonedDateTime timestamp = instance.from; timestamp.isBefore(instance.to) || timestamp.isEqual(instance.to); timestamp = timestamp.plus(1, instance.baseTimeUnit)) {
                    indices.add(formatName(timestamp));
                }
            }
            return indices;
        }

        @Override
        public String single() {
            return formatName(instance.at);
        }

        private String formatName(ZonedDateTime timestamp) {
            return format(
                    "%s:%s:%s%s",
                    instance.owner,
                    instance.seriesName,
                    measurementDistanceName(instance.measurementDistance),
                    timestamp != null ? dateTimeFormatter(instance.baseTimeUnit).format(timestamp) : "*"
            );
        }

    }

    private static ZonedDateTime truncate(ZonedDateTime timestamp, ChronoUnit toUnit) {
        switch (toUnit) {
            case YEARS:
                return ZonedDateTime.of(timestamp.getYear(), 1, 1, 0, 0, 0, 0, timestamp.getZone());
            case MONTHS:
                return ZonedDateTime.of(timestamp.getYear(), timestamp.getMonthValue(), 1, 0, 0, 0, 0, timestamp.getZone());
            case DAYS:
                return ZonedDateTime.of(timestamp.getYear(), timestamp.getMonthValue(), timestamp.getDayOfMonth(), 0, 0, 0, 0, timestamp.getZone());
        }
        return timestamp.truncatedTo(toUnit);
    }

    private static String measurementDistanceName(ChronoUnit chronoUnit) {
        switch (chronoUnit) {
            case MINUTES:
                return "minute";
            case HOURS:
                return "hour";
            case DAYS:
                return "day";
            case MONTHS:
                return "month";
            case YEARS:
                return "year";
        }
        throw new IllegalArgumentException(chronoUnit.toString());
    }

    private static DateTimeFormatter dateTimeFormatter(ChronoUnit baseTimeUnit) {
        switch (baseTimeUnit) {
            case YEARS:
                return DateTimeFormatter.ofPattern("yyyy");
            case DAYS:
                return DateTimeFormatter.ofPattern("yyyy.MM.dd");
            case FOREVER:
                return DateTimeFormatter.ofPattern("");
            default:
                throw new RuntimeException("Unsupported period unit for series: " + baseTimeUnit);
        }
    }


}