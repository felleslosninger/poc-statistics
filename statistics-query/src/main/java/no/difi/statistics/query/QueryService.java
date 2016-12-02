package no.difi.statistics.query;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;

import java.time.ZonedDateTime;
import java.util.List;

public interface QueryService {

    List<String> availableTimeSeries(String owner);

    TimeSeriesPoint last(String seriesName, MeasurementDistance distance, String owner, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> minutes(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> query(String seriesName, MeasurementDistance distance, String owner, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter);

    List<TimeSeriesPoint> hours(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> days(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> months(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> lastPerDistance(String seriesName, MeasurementDistance distance, MeasurementDistance targetDistance, String owner, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> years(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to);

    TimeSeriesPoint sum(String seriesName, MeasurementDistance distance, String owner, ZonedDateTime from, ZonedDateTime to);

}