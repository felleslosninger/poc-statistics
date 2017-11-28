package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.FOREVER;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.util.Collections.singletonList;
import static no.difi.statistics.elasticsearch.Timestamp.truncatedTimestamp;

public class IndexNameResolver {

    private TimeSeriesDefinition seriesDefinition;
    private ChronoUnit baseTimeUnit;
    private ZonedDateTime from;
    private ZonedDateTime to;
    private ZonedDateTime at;

    public static SeriesDefinitionEntry resolveIndexName() {
        return new Fluent();
    }

    public interface FromEntry {
        ToEntry from(ZonedDateTime from);
    }

    public interface ToEntry {
        ResolveList to(ZonedDateTime to);
    }

    public interface FromOrAtOrResolveList extends FromEntry, AtEntry, ResolveList {
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
        FromOrAtOrResolveList seriesDefinition(TimeSeriesDefinition seriesDefinition);
    }

    private static class Fluent implements
            FromEntry,
            ToEntry,
            FromOrAtOrResolveList,
            AtEntry,
            ResolveList,
            ResolveSingle,
            SeriesDefinitionEntry {

        private IndexNameResolver instance = new IndexNameResolver();

        @Override
        public ToEntry from(ZonedDateTime from) {
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
        public FromOrAtOrResolveList seriesDefinition(TimeSeriesDefinition seriesDefinition) {
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
                return singletonList(formatName(instance.from));
            List<String> indices = new ArrayList<>();
            if (instance.from == null && instance.to == null) {
                indices.add(formatName(null));
            } else {
                if (instance.from == null) instance.from = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
                if (instance.to == null) instance.to = ZonedDateTime.of(2050, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
                instance.from = truncatedTimestamp(instance.from, instance.baseTimeUnit);
                instance.to = truncatedTimestamp(instance.to, instance.baseTimeUnit);
                for (ZonedDateTime timestamp = instance.from; timestamp.isBefore(instance.to) || timestamp.isEqual(instance.to); timestamp = timestamp.plus(1, instance.baseTimeUnit)) {
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
                    "%s:%s:%s%s",
                    instance.seriesDefinition.getOwner(),
                    instance.seriesDefinition.getName(),
                    measurementDistanceName(instance.seriesDefinition.getDistance()),
                    timestamp != null ? dateTimeFormatter(instance.baseTimeUnit).format(timestamp) : "*"
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