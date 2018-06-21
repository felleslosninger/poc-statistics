package no.difi.statistics.query.elasticsearch.commands;

import no.difi.statistics.elasticsearch.Timestamp;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.query.model.QueryFilter;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.List;
import java.util.Map;

import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.sumPerTimestampAggregation;
import static org.elasticsearch.search.aggregations.BucketOrder.key;

public class LastQuery extends SinglePointQuery {

    private TimeSeriesDefinition seriesDefinition;
    private QueryFilter queryFilter;
    private GetMeasurementIdentifiers.Builder getMeasurementIdentifiersCommand;

    @Override
    public TimeSeriesPoint execute() {
        return last(
                resolveIndexName().seriesDefinition(seriesDefinition).range(queryFilter.timeRange()).list(),
                queryFilter
        );
    }

    private TimeSeriesPoint last(List<String> indexNames, QueryFilter queryFilter) {
        SearchResponse response = search(searchRequest(
                indexNames,
                queryFilter,
                null,
                0,
                sumPerTimestampAggregation("last", measurementIds(indexNames)).order(key(false)).size(1)
        ));
        return pointFromLastAggregation(response, queryFilter.categories());
    }

    private List<String> measurementIds(List<String> indexNames) {
        return getMeasurementIdentifiersCommand.indexNames(indexNames).execute();
    }

    private TimeSeriesPoint pointFromLastAggregation(SearchResponse response, Map<String, String> categories) {
        if (response.getAggregations() == null)
            return null;
        if (response.getAggregations().get("last") == null)
            throw new RuntimeException("No last aggregation in result");
        if (response.getAggregations().<Terms>get("last").getBuckets().size() == 0)
            return null;
        if (response.getAggregations().<Terms>get("last").getBuckets().size() > 1)
            throw new RuntimeException("Too many buckets in last aggregation: "
                    + response.getAggregations().<Terms>get("last").getBuckets().size());
        Terms.Bucket bucket = response.getAggregations().<Terms>get("last").getBuckets().get(0);
        return TimeSeriesPoint.builder()
                .timestamp(Timestamp.parse(bucket.getKeyAsString()))
                .measurements(measurementsFromSumAggregations(bucket.getAggregations()))
                .categories(categories)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private LastQuery instance = new LastQuery();

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

        public LastQuery build() {
            return instance;
        }

    }


}
