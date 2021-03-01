package no.difi.statistics.query.elasticsearch.commands;

import no.difi.statistics.elasticsearch.Timestamp;
import no.difi.statistics.model.TimeRange;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.query.model.QueryFilter;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.sort.SortOrder;

import java.time.ZonedDateTime;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.sumPerTimestampAggregation;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.search.aggregations.BucketOrder.key;

public class SumQuery extends SinglePointQuery {

    private static final String timestampField = "timestamp";
    private TimeSeriesDefinition seriesDefinition;
    private QueryFilter queryFilter;
    private GetMeasurementIdentifiers.Builder getMeasurementIdentifiersCommand;

    @Override
    public TimeSeriesPoint execute() {
        return sumAggregate(
                resolveIndexName().seriesDefinition(seriesDefinition).range(queryFilter.timeRange()).list(),
                queryFilter
        );
    }

    private TimeSeriesPoint sumAggregate(List<String> indexNames, QueryFilter queryFilter) {
        if (queryFilter.timeRange() == null)
            return sumAggregateUnbounded(indexNames, queryFilter);
        SearchResponse response = search(searchRequest(
                indexNames,
                queryFilter,
                null,
                0,
                sumAggregation("a", queryFilter.timeRange(), measurementIds(indexNames))

        ));
        if (response.getAggregations() == null)
            return null;
        return sumPointFromRangeBucket(response.getAggregations().get("a")).categories(queryFilter.categories()).build();
    }

    private static DateRangeAggregationBuilder sumAggregation(String name, TimeRange timeRange, List<String> measurementIds) {
        DateRangeAggregationBuilder builder = dateRange(name).field(timestampField);
        if (timeRange.from() == null)
            builder.addUnboundedTo(Timestamp.format(timeRange.to()));
        else if (timeRange.to() == null)
            builder.addUnboundedFrom(Timestamp.format(timeRange.from()));
        else
            builder.addRange(Timestamp.format(timeRange.from()), Timestamp.format(timeRange.to()));
        for (String measurementId : measurementIds)
            builder.subAggregation(sum(measurementId).field(measurementId));
        builder.subAggregation(topHits("last").size(1).sort(timestampField, SortOrder.DESC)); // For timestamp on sum point
        return builder;
    }

    private TimeSeriesPoint sumAggregateUnbounded(List<String> indexNames, QueryFilter queryFilter) {
        List<String> measurementIds = measurementIds(indexNames);
        List<AggregationBuilder> aggregations = measurementIds.stream().map(mid -> AggregationBuilders.sum(mid).field(mid)).collect(toList());
        aggregations.add(sumPerTimestampAggregation("last", measurementIds).order(key(false)).size(1));
        SearchResponse response = search(searchRequest(
                indexNames,
                queryFilter,
                null,
                0,
                aggregations.toArray(new AggregationBuilder[0])
        ));
        return sumPoint(response.getAggregations()).categories(queryFilter.categories()).build();
    }

    private TimeSeriesPoint.Builder sumPointFromRangeBucket(Range range) {
        if (range == null)
            return null;
        Range.Bucket bucket = range.getBuckets().get(0);
        Aggregations aggregations = bucket.getAggregations();
        ZonedDateTime timestamp = timestamp(aggregations.<TopHits>get("last").getHits().getAt(0));
        return TimeSeriesPoint.builder().timestamp(timestamp).measurements(measurementsFromSumAggregations(aggregations));
    }

    private TimeSeriesPoint.Builder sumPoint(Aggregations aggregations) {
        if (aggregations == null)
            return null;
        ZonedDateTime timestamp = timestamp(aggregations.<Terms>get("last").getBuckets().get(0));
        return TimeSeriesPoint.builder().timestamp(timestamp).measurements(measurementsFromSumAggregations(aggregations));
    }

    private List<String> measurementIds(List<String> indexNames) {
        return getMeasurementIdentifiersCommand.indexNames(indexNames).execute();
    }

    private static ZonedDateTime timestamp(MultiBucketsAggregation.Bucket bucket) {
        return Timestamp.parse(bucket.getKeyAsString());
    }

    public static ZonedDateTime timestamp(SearchHit hit) {
        return Timestamp.parse(hit.getSourceAsMap().get(timestampField).toString());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private SumQuery instance = new SumQuery();

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

        public SumQuery build() {
            return instance;
        }

    }


}
