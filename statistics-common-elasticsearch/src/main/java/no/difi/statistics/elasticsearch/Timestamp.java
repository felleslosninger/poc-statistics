package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;

public class Timestamp {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public static String format(ZonedDateTime timestamp) {
        return dateTimeFormatter.format(timestamp);
    }

    public static ZonedDateTime parse(String value) {
        return ZonedDateTime.parse(value, dateTimeFormatter);
    }

    public static ZonedDateTime truncatedTimestamp(ZonedDateTime timestamp, ChronoUnit toUnit) {
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

    public static ZonedDateTime truncatedTimestamp(ZonedDateTime timestamp, MeasurementDistance distance) {
        return truncatedTimestamp(timestamp, chronoUnit(distance));
    }

    public static ZonedDateTime normalize(ZonedDateTime timestamp, MeasurementDistance distance) {
        return truncatedTimestamp(timestamp, distance).withZoneSameInstant(UTC);
    }

    private static ChronoUnit chronoUnit(MeasurementDistance distance) {
        switch (distance) {
            case minutes: return MINUTES;
            case hours: return HOURS;
            case days: return DAYS;
            case months: return MONTHS;
            case years: return YEARS;
            default: throw new IllegalArgumentException("Unsupported measurement distance: " + distance);
        }
    }

}
