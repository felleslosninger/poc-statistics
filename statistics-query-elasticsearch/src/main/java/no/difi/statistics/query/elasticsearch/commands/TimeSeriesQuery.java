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

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.summarizeMeasurements;
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
                0,
                summarizeMeasurements("categoryAggregation", getMeasurementIdentifiersCommand.indexNames(indexNames).execute(), queryFilter.perCategory())
        ));
        if (response.getAggregations() != null)
            return points(response.getAggregations().get("categoryAggregation"), queryFilter);
        else
            return emptyList();
    }

    private List<TimeSeriesPoint> points(MultiBucketsAggregation aggregation, QueryFilter queryFilter) {
        return points(aggregation.getBuckets().stream(), queryFilter.perCategory())
                .map(p -> p.categories(queryFilter.categories()).build())
                .collect(toList());
    }

    private Stream<TimeSeriesPoint.Builder> points(Stream<? extends MultiBucketsAggregation.Bucket> bucketStream, String categoryKey) {
        if (categoryKey != null)
            return bucketStream
                    .map(bucket -> pointPerCategoryValue(categoryAggregation(bucket), Timestamp.parse(bucket.getKeyAsString()), categoryKey))
                    .flatMap(Collection::stream);
        else
            return bucketStream
                    .map(bucket -> point(bucket, Timestamp.parse(bucket.getKeyAsString())));
    }

    private List<TimeSeriesPoint.Builder> pointPerCategoryValue(
            MultiBucketsAggregation aggregation,
            ZonedDateTime timestamp,
            String categoryKey
    ) {
        return aggregation.getBuckets().stream()
                .map(bucket -> point(bucket, timestamp).category(categoryKey, bucket.getKeyAsString()))
                .collect(toList());
    }

    private TimeSeriesPoint.Builder point(MultiBucketsAggregation.Bucket bucket, ZonedDateTime timestamp) {
        return TimeSeriesPoint.builder().timestamp(timestamp).measurements(measurementsFromSumAggregations(bucket.getAggregations()));
    }

    private MultiBucketsAggregation categoryAggregation(MultiBucketsAggregation.Bucket bucket) {
        return (MultiBucketsAggregation)bucket.getAggregations().iterator().next();
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