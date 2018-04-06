package no.difi.statistics.query.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.QueryFilter;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.sort.SortOrder.ASC;

public class GetSumHistogram {

    private static final String timeFieldName = "timestamp";
    private static final String indexType = "default";
    private RestHighLevelClient elasticsearchClient;
    private TimeSeriesDefinition seriesDefinition;
    private QueryFilter queryFilter;
    private MeasurementDistance targetDistance;
    private GetMeasurementIdentifiers.Builder getMeasurementIdentifiersCommand;

    private List<TimeSeriesPoint> doExecute() {
        return sumPerDistance(
                resolveIndexName().seriesDefinition(seriesDefinition).from(queryFilter.from()).to(queryFilter.to()).list(),
                targetDistance,
                queryFilter
        );
    }

    private List<TimeSeriesPoint> sumPerDistance(List<String> indexNames, MeasurementDistance targetDistance, QueryFilter queryFilter) {
        SearchSourceBuilder searchSource = searchSource(queryFilter)
                .aggregation(sumPerDistanceAggregation(targetDistance, getMeasurementIdentifiersCommand.indexNames(indexNames).execute()))
                .size(0); // We are after aggregation and not the search hits
        SearchResponse response = search(indexNames, searchSource);
        if (response.getAggregations() != null)
            return points(response.getAggregations().get(targetDistance.name()), queryFilter.categories());
        else
            return emptyList();
    }


    private SearchResponse search(List<String> indexNames, SearchSourceBuilder searchSource) {
        try {
            return elasticsearchClient.search(searchRequest(indexNames).source(searchSource.sort(timeFieldName, ASC)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to search", e);
        }
    }

    private SearchRequest searchRequest(List<String> indexNames) {
        return new SearchRequest(indexNames.toArray(new String[indexNames.size()]))
                .indicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .types(indexType);
    }

    private SearchSourceBuilder searchSource(QueryFilter queryFilter) {
        BoolQueryBuilder boolQuery = boolQuery();
        boolQuery.filter(timeRangeQuery(queryFilter.from(), queryFilter.to()));
        queryFilter.categories().forEach((k, v) -> boolQuery.filter(categoryQuery(k, v)));
        return SearchSourceBuilder.searchSource().query(boolQuery);
    }

    private DateHistogramAggregationBuilder sumPerDistanceAggregation(MeasurementDistance targetDistance, List<String> measurementIds) {
        DateHistogramAggregationBuilder builder = dateHistogram(targetDistance.name()).field(timeFieldName).dateHistogramInterval(dateHistogramInterval(targetDistance));
        for (String measurementId : measurementIds)
            builder.subAggregation(sum(measurementId).field(measurementId));
        return builder;
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
        return ZonedDateTime.parse(bucket.getKeyAsString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    private Map<String, Long> measurements(MultiBucketsAggregation.Bucket bucket) {
        Map<String, Long> measurements = new HashMap<>();
        for (Aggregation aggregation : bucket.getAggregations()) {
            measurements.put(aggregation.getName(), (long) ((Sum) aggregation).getValue());
        }
        return measurements;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private GetSumHistogram instance = new GetSumHistogram();

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

        public List<TimeSeriesPoint> execute() {
            return instance.doExecute();
        }

    }


}
