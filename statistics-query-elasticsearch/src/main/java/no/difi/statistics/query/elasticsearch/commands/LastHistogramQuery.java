package no.difi.statistics.query.elasticsearch.commands;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.query.model.QueryFilter;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Sum;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.sumPerTimestampAggregation;
import static org.elasticsearch.search.aggregations.BucketOrder.key;

public class LastHistogramQuery extends HistogramQuery {

    private TimeSeriesDefinition seriesDefinition;
    private QueryFilter queryFilter;
    private MeasurementDistance targetDistance;
    private GetMeasurementIdentifiers.Builder getMeasurementIdentifiersCommand;

    public List<TimeSeriesPoint> execute() {
        return lastPerDistance(
                resolveIndexName().seriesDefinition(seriesDefinition).range(queryFilter.timeRange()).list(),
                targetDistance,
                queryFilter
        );
    }

    private List<TimeSeriesPoint> lastPerDistance(List<String> indexNames, MeasurementDistance targetDistance, QueryFilter queryFilter) {
        SearchResponse response = search(searchRequest(
                indexNames,
                queryFilter,
                null,
                0,
                lastPerDistanceAggregation(targetDistance, getMeasurementIdentifiersCommand.indexNames(indexNames).execute())
        ));
        if (response.getAggregations() != null)
            return points(response.getAggregations().get(targetDistance.name()), queryFilter.categories());
        else
            return emptyList();
    }

    private DateHistogramAggregationBuilder lastPerDistanceAggregation(MeasurementDistance targetDistance, List<String> measurementIds) {
        return dateHistogram(targetDistance).subAggregation(
                sumPerTimestampAggregation("sumPerTimestamp", measurementIds).order(key(false)).size(1)
        );
    }

    private List<TimeSeriesPoint> points(MultiBucketsAggregation aggregation, Map<String, String> categories) {
        return aggregation.getBuckets().stream()
                .map(this::point)
                .map(p -> p.categories(categories).build())
                .collect(toList());
    }

    private TimeSeriesPoint.Builder point(MultiBucketsAggregation.Bucket bucket) {
        return TimeSeriesPoint.builder().timestamp(timestamp(bucket)).measurements(measurements(bucket));
    }

    private ZonedDateTime timestamp(MultiBucketsAggregation.Bucket bucket) {
        return ZonedDateTime.parse(timestampAggregation(bucket).getBuckets().get(0).getKeyAsString());
    }

    private Map<String, Long> measurements(MultiBucketsAggregation.Bucket bucket) {
        Map<String, Long> measurements = new HashMap<>();
        for (Aggregation aggregation : timestampAggregation(bucket).getBuckets().get(0).getAggregations()) {
            measurements.put(aggregation.getName(), (long) ((Sum) aggregation).getValue());
        }
        return measurements;
    }

    private Terms timestampAggregation(MultiBucketsAggregation.Bucket bucket) {
        return bucket.getAggregations().get("sumPerTimestamp");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private LastHistogramQuery instance = new LastHistogramQuery();

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

        public Builder targetDistance(MeasurementDistance targetDistance) {
            instance.targetDistance = targetDistance;
            return this;
        }

        public Builder measurementIdentifiersCommand(GetMeasurementIdentifiers.Builder command) {
            instance.getMeasurementIdentifiersCommand = command;
            return this;
        }

        public LastHistogramQuery build() {
            return instance;
        }

    }

}
