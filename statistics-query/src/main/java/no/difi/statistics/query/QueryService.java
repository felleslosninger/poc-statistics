package no.difi.statistics.query;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;

import java.time.ZonedDateTime;
import java.util.List;

public interface QueryService {

    List<TimeSeriesDefinition> availableTimeSeries();

    TimeSeriesPoint last(TimeSeriesDefinition seriesDefinition, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> query(TimeSeriesDefinition seriesDefinition, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> query(TimeSeriesDefinition seriesDefinition, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter);

    List<TimeSeriesPoint> lastPerDistance(TimeSeriesDefinition seriesDefinition, MeasurementDistance targetDistance, ZonedDateTime from, ZonedDateTime to);

    TimeSeriesPoint sum(TimeSeriesDefinition seriesDefinition, ZonedDateTime from, ZonedDateTime to);

    List<TimeSeriesPoint> sumPerDistance(TimeSeriesDefinition seriesDefinition, MeasurementDistance targetDistance, ZonedDateTime from, ZonedDateTime to);
}