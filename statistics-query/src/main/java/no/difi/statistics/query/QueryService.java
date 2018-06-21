package no.difi.statistics.query;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.PercentileFilter;
import no.difi.statistics.query.model.QueryFilter;

import java.util.List;

public interface QueryService {

    List<TimeSeriesDefinition> availableTimeSeries();

    TimeSeriesPoint last(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter);

    List<TimeSeriesPoint> query(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter);

    List<TimeSeriesPoint> query(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter, PercentileFilter filter);

    List<TimeSeriesPoint> lastHistogram(TimeSeriesDefinition seriesDefinition, MeasurementDistance targetDistance, QueryFilter queryFilter);

    TimeSeriesPoint sum(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter);

    List<TimeSeriesPoint> sumHistogram(TimeSeriesDefinition seriesDefinition, MeasurementDistance targetDistance, QueryFilter queryFilter);
}