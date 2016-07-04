package no.difi.statistics.util;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.util.Collections.singletonList;

public class IndexNameResolver {

    public static List<String> resolveMinuteIndexNames(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return indexNamesForSeriesPerDay(seriesName, MINUTES, from, to);
    }

    public static List<String> resolveHourIndexNames(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return indexNamesForSeriesPerDay(seriesName, HOURS, from, to);
    }

    public static List<String> resolveDayIndexNames(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return indexNamesForSeriesPerYear(seriesName, DAYS, from, to);
    }

    public static List<String> resolveMonthIndexNames(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return indexNamesForSeriesPerYear(seriesName, MONTHS, from, to);
    }

    public static List<String> resolveYearIndexNames(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return singletonList(format("%s:%s", seriesName, toIndexPart(YEARS)));
    }

    private static List<String> indexNamesForSeriesPerDay(String seriesName, ChronoUnit seriesTimeUnit, ZonedDateTime from, ZonedDateTime to) {
        return indexNamesForSeries(seriesName, seriesTimeUnit, DAYS, "yyyy.MM.dd", from, to);
    }

    private static List<String> indexNamesForSeriesPerYear(String seriesName, ChronoUnit seriesTimeUnit, ZonedDateTime from, ZonedDateTime to) {
        return indexNamesForSeries(seriesName, seriesTimeUnit, YEARS, "yyyy", from, to);
    }

    private static List<String> indexNamesForSeries(String seriesName, ChronoUnit seriesTimeUnit, ChronoUnit indexTimeUnit, String indexTimeFormat, ZonedDateTime from, ZonedDateTime to) {
        List<String> indices = new ArrayList<>();
        from = truncate(from, indexTimeUnit);
        to = truncate(to, indexTimeUnit);
        for (ZonedDateTime time = from; time.isBefore(to) || time.isEqual(to); time = time.plus(1, indexTimeUnit)) {
            indices.add(indexNameForSeries(seriesName, seriesTimeUnit, indexTimeFormat, time));
        }
        return indices;
    }

    private static String indexNameForSeries(String seriesName, ChronoUnit seriesTimeUnit, String indexTimeFormat, ZonedDateTime time) {
        return format(
                "%s:%s%s",
                seriesName,
                toIndexPart(seriesTimeUnit),
                DateTimeFormatter.ofPattern(indexTimeFormat).format(time)
        );
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

    private static String toIndexPart(ChronoUnit chronoUnit) {
        switch (chronoUnit) {
            case MINUTES: return "minute";
            case HOURS: return "hour";
            case DAYS: return "day";
            case MONTHS: return "month";
            case YEARS: return "year";
        }
        throw new IllegalArgumentException(chronoUnit.toString());
    }
    
}
