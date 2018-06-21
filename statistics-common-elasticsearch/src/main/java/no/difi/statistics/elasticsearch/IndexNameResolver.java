package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeRange;
import no.difi.statistics.model.TimeSeriesDefinition;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.FOREVER;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.util.Collections.singletonList;
import static no.difi.statistics.elasticsearch.Timestamp.truncatedTimestamp;

public class IndexNameResolver {

    private static String partSeparator = "@";
    private static Pattern pattern = Pattern.compile("(.+)@(.+)@(minute|hour|day|month|year).*");
    private TimeSeriesDefinition seriesDefinition;
    private ChronoUnit baseTimeUnit;
    private TimeRange timeRange;
    private ZonedDateTime at;

    public static Pattern pattern() {
        return pattern;
    }

    public static String generic(String indexName) {
        return indexName.substring(0, indexName.lastIndexOf(partSeparator) + 1) + "*";
    }

    public static SeriesDefinitionEntry resolveIndexName() {
        return new Fluent();
    }

    public interface TimeRangeEntry {
        ResolveList range(TimeRange range);
    }

    public interface TimeRangeOrAtOrResolveList extends TimeRangeEntry, AtEntry, ResolveList {
    }

    public interface AtEntry {
        ResolveSingle at(ZonedDateTime at);
    }

    public interface ResolveList {
        List<String> list();
    }

    public interface ResolveSingle {
        String single();
    }

    public interface SeriesDefinitionEntry {
        TimeRangeOrAtOrResolveList seriesDefinition(TimeSeriesDefinition seriesDefinition);
    }

    private static class Fluent implements
            TimeRangeEntry,
            TimeRangeOrAtOrResolveList,
            AtEntry,
            ResolveList,
            ResolveSingle,
            SeriesDefinitionEntry {

        private IndexNameResolver instance = new IndexNameResolver();

        @Override
        public ResolveList range(TimeRange range) {
            instance.timeRange = range;
            return this;
        }

        @Override
        public ResolveSingle at(ZonedDateTime at) {
            instance.at = at;
            return this;
        }

        @Override
        public TimeRangeOrAtOrResolveList seriesDefinition(TimeSeriesDefinition seriesDefinition) {
            instance.seriesDefinition = seriesDefinition;
            switch (seriesDefinition.getDistance()) {
                case minutes:
                case hours:
                case days:
                case months:
                    instance.baseTimeUnit = YEARS;
                    break;
                case years:
                    instance.baseTimeUnit = FOREVER;
                    break;
            }
            return this;
        }


        @Override
        public List<String> list() {
            if (instance.baseTimeUnit == FOREVER)
                return singletonList(formatName(instance.timeRange != null ? instance.timeRange.from() : null));
            List<String> indices = new ArrayList<>();
            if (instance.timeRange == null) {
                indices.add(formatName(null));
            } else {
                ZonedDateTime from = instance.timeRange.from();
                ZonedDateTime to = instance.timeRange.to();
                if (from == null)
                    from = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
                if (to == null)
                    to = ZonedDateTime.of(2050, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
                from = truncatedTimestamp(from, instance.baseTimeUnit);
                to = truncatedTimestamp(to, instance.baseTimeUnit);
                for (ZonedDateTime timestamp = from; timestamp.isBefore(to) || timestamp.isEqual(to); timestamp = timestamp.plus(1, instance.baseTimeUnit)) {
                    if (indices.size() >= 10) {
                        // Use wildcard instead if number of indices exceeds 10
                        indices.clear();
                        indices.add(formatName(null));
                        break;
                    }
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
                    "%1$s%5$s%2$s%5$s%3$s%4$s", // <owner><partSeparator><seriesName><partSeparator><distance><timeUnit>
                    instance.seriesDefinition.getOwner(),
                    instance.seriesDefinition.getName(),
                    measurementDistanceName(instance.seriesDefinition.getDistance()),
                    timestamp != null ? dateTimeFormatter(instance.baseTimeUnit).format(timestamp) : "*",
                    partSeparator
            );
        }

    }

    private static String measurementDistanceName(MeasurementDistance distance) {
        switch (distance) {
            case minutes:
                return "minute";
            case hours:
                return "hour";
            case days:
                return "day";
            case months:
                return "month";
            case years:
                return "year";
        }
        throw new IllegalArgumentException(distance.toString());
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