package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;

import java.time.ZonedDateTime;

import static java.time.ZoneOffset.UTC;
import static no.difi.statistics.elasticsearch.Timestamp.truncate;

public class IdResolver {

    public static String id(TimeSeriesPoint dataPoint, MeasurementDistance distance) {
        return format(dataPoint.getTimestamp(), distance);
    }

    private static String format(ZonedDateTime timestamp, MeasurementDistance distance) {
        return normalize(timestamp, distance).toString();
    }

    private static ZonedDateTime normalize(ZonedDateTime timestamp, MeasurementDistance distance) {
        return truncate(timestamp, distance).withZoneSameInstant(UTC);
    }

}
