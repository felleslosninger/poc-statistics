package no.difi.statistics.query.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.PercentileFilter;
import no.difi.statistics.query.model.QueryFilter;
import no.difi.statistics.query.QueryService;

import java.util.List;

public class ElasticsearchQueryService implements QueryService {

    private CommandFactory commandFactory;

    public ElasticsearchQueryService(CommandFactory commandFactory) {
        this.commandFactory = commandFactory;
    }

    @Override
    public List<TimeSeriesDefinition> availableTimeSeries() {
        return commandFactory.availableTimeSeries().build().execute();
    }

    @Override
    public List<TimeSeriesPoint> query(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter) {
        return commandFactory.query()
                .seriesDefinition(seriesDefinition).queryFilter(queryFilter)
                .measurementIdentifiersCommand(commandFactory.measurementIdentifiers()).build().execute();
    }

    @Override
    public List<TimeSeriesPoint> query(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter, PercentileFilter filter) {
        return commandFactory.percentile()
                .seriesDefinition(seriesDefinition).queryFilter(queryFilter).percentileFilter(filter).build().execute();
    }

    @Override
    public List<TimeSeriesPoint> lastHistogram(
            TimeSeriesDefinition seriesDefinition,
            MeasurementDistance targetDistance,
            QueryFilter queryFilter
    ){
        return commandFactory.lastHistogram()
                .seriesDefinition(seriesDefinition).targetDistance(targetDistance).queryFilter(queryFilter)
                .measurementIdentifiersCommand(commandFactory.measurementIdentifiers()).build().execute();
    }

    @Override
    public TimeSeriesPoint sum(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter) {
        return commandFactory.sum()
                .seriesDefinition(seriesDefinition).queryFilter(queryFilter)
                .measurementIdentifiersCommand(commandFactory.measurementIdentifiers()).build().execute();
    }

    @Override
    public List<TimeSeriesPoint> sumHistogram(TimeSeriesDefinition seriesDefinition, MeasurementDistance targetDistance, QueryFilter queryFilter) {
        return commandFactory.sumHistogram()
                .seriesDefinition(seriesDefinition).targetDistance(targetDistance).queryFilter(queryFilter)
                .measurementIdentifiersCommand(commandFactory.measurementIdentifiers()).build().execute();
    }

    @Override
    public TimeSeriesPoint last(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter) {
        return commandFactory.last()
                .seriesDefinition(seriesDefinition).queryFilter(queryFilter)
                .measurementIdentifiersCommand(commandFactory.measurementIdentifiers()).build().execute();
    }

}
