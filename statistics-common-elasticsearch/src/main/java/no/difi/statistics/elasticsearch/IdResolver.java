package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;

import java.io.UnsupportedEncodingException;
import java.time.ZonedDateTime;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.joining;
import static no.difi.statistics.elasticsearch.Timestamp.truncate;

public class IdResolver {

    public static String id(TimeSeriesPoint dataPoint, TimeSeriesDefinition seriesDefinition) {
        return nameUUID(format(dataPoint.getTimestamp(), seriesDefinition));
    }

    private static String nameUUID(String name) {
        try {
            return UUID.nameUUIDFromBytes(name.getBytes("UTF-8")).toString();
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is unsupported", e);
        }
    }

    private static String format(ZonedDateTime timestamp, TimeSeriesDefinition seriesDefinition) {
        return normalizeTimestamp(timestamp, seriesDefinition.getDistance()).toString() + categoriesAsString(seriesDefinition);
    }

    private static String categoriesAsString(TimeSeriesDefinition seriesDefinition) {
        return seriesDefinition.getCategories().map(cs ->
            cs.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).sorted().collect(joining("&"))
        ).orElse("");
    }


    private static ZonedDateTime normalizeTimestamp(ZonedDateTime timestamp, MeasurementDistance distance) {
        return truncate(timestamp, distance).withZoneSameInstant(UTC);
    }

}
