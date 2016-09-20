package no.difi.statistics.query;

import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;

import java.time.ZonedDateTime;
import java.util.List;

public interface QueryService {

    List<TimeSeriesPoint> minutes(String seriesName, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> minutes(String seriesName, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter);

    List<TimeSeriesPoint> hours(String seriesName, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> days(String seriesName, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> months(String seriesName, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> monthsSnapshot(String seriesName, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> years(String seriesName, ZonedDateTime from, ZonedDateTime to);

    TimeSeriesPoint point(String seriesName, ZonedDateTime from, ZonedDateTime to);

}