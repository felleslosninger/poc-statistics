package no.difi.statistics.query.elasticsearch.commands;

import no.difi.statistics.elasticsearch.Timestamp;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.query.model.QueryFilter;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.sumPerTimestampAggregation;
import static no.difi.statistics.model.MeasurementDistance.*;

public class TimeSeriesQuery extends Query {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private TimeSeriesDefinition seriesDefinition;
    private QueryFilter queryFilter;
    private SumHistogramQuery.Builder getSumHistogramCommand;
    private GetMeasurementIdentifiers.Builder getMeasurementIdentifiersCommand;

    public List<TimeSeriesPoint> execute() {
        List<TimeSeriesPoint> result = search(
                resolveIndexName().seriesDefinition(seriesDefinition).range(queryFilter.timeRange()).list(),
                queryFilter
        );
        if (result.isEmpty() && seriesDefinition.getDistance().equals(days)) {
            logger.info("Empty result for day series search. Attempting to aggregate minute series...");
            seriesDefinition = TimeSeriesDefinition.builder().name(seriesDefinition.getName()).distance(minutes).owner(seriesDefinition.getOwner());
            result = getSumHistogramCommand
                    .seriesDefinition(seriesDefinition).targetDistance(days).queryFilter(queryFilter)
                    .measurementIdentifiersCommand(getMeasurementIdentifiersCommand).build().execute();
        } else if (result.isEmpty() && seriesDefinition.getDistance().equals(months)) {
            logger.info("Empty result for month series search. Attempting to aggregate minute series...");
            seriesDefinition = TimeSeriesDefinition.builder().name(seriesDefinition.getName()).distance(minutes).owner(seriesDefinition.getOwner());
            result = getSumHistogramCommand
                    .seriesDefinition(seriesDefinition).targetDistance(months).queryFilter(queryFilter)
                    .measurementIdentifiersCommand(getMeasurementIdentifiersCommand).build().execute();
        }
        return result;
    }

    private List<TimeSeriesPoint> search(List<String> indexNames, QueryFilter queryFilter) {
        SearchResponse response = search(searchRequest(
                indexNames,
                queryFilter,
                null,
                10_000,
                sumPerTimestampAggregation("categoryAggregation", getMeasurementIdentifiersCommand.indexNames(indexNames).execute())
        ));
        if (response.getAggregations() != null)
            return points(response.getAggregations().get("categoryAggregation"), queryFilter.categories());
        else
            return emptyList();
    }

    private List<TimeSeriesPoint> points(MultiBucketsAggregation aggregation, Map<String, String> categories) {
        return aggregation.getBuckets().stream()
                .map(this::point)
                .map(p -> p.categories(categories).build())
                .collect(toList());
    }

    private TimeSeriesPoint.Builder point(MultiBucketsAggregation.Bucket bucket) {
        return TimeSeriesPoint.builder().timestamp(Timestamp.parse(bucket.getKeyAsString())).measurements(measurementsFromSumAggregations(bucket.getAggregations()));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private TimeSeriesQuery instance = new TimeSeriesQuery();

        public Builder elasticsearchClient(RestHighLevelClient client) {
            instance.elasticsearchClient = client;
            return this;
        }

        public Builder seriesDefinition(TimeSeriesDefinition seriesDefinition) {
            instance.seriesDefinition = seriesDefinition;
            return this;
        }

        public Builder queryFilter(QueryFilter queryFilter) {
            instance.queryFilter = queryFilter;
            return this;
        }

        public Builder measurementIdentifiersCommand(GetMeasurementIdentifiers.Builder command) {
            instance.getMeasurementIdentifiersCommand = command;
            return this;
        }

        public Builder sumHistogramCommand(SumHistogramQuery.Builder command) {
            instance.getSumHistogramCommand = command;
            return this;
        }

        public TimeSeriesQuery build() {
            return instance;
        }

    }

}