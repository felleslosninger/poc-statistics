package no.difi.statistics;

import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;

import java.time.ZonedDateTime;
import java.util.List;

public interface QueryService {

    List<TimeSeriesPoint> minutes(String seriesName, String type, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> minutes(String seriesName, String type, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter);

    List<TimeSeriesPoint> hours(String seriesName, String type, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> days(String seriesName, String type, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> months(String seriesName, String type, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> years(String seriesName, String type, ZonedDateTime from, ZonedDateTime to);
}
