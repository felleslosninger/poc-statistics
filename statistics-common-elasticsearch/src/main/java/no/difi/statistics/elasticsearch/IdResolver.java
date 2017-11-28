package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;

import java.io.UnsupportedEncodingException;
import java.time.ZonedDateTime;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.joining;
import static no.difi.statistics.elasticsearch.Timestamp.truncatedTimestamp;

public class IdResolver {

    public static String id(TimeSeriesPoint dataPoint, TimeSeriesDefinition seriesDefinition) {
        return nameUUID(
                normalizeTimestamp(dataPoint.getTimestamp(), seriesDefinition.getDistance()).toString()
                        + categoriesAsString(dataPoint)
        );
    }

    private static String nameUUID(String name) {
        try {
            return UUID.nameUUIDFromBytes(name.getBytes("UTF-8")).toString();
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is unsupported", e);
        }
    }

    private static String categoriesAsString(TimeSeriesPoint dataPoint) {
        return dataPoint.getCategories().map(cs ->
            cs.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).sorted().collect(joining("&"))
        ).orElse("");
    }

    private static ZonedDateTime normalizeTimestamp(ZonedDateTime timestamp, MeasurementDistance distance) {
        return truncatedTimestamp(timestamp, distance).withZoneSameInstant(UTC);
    }

}
