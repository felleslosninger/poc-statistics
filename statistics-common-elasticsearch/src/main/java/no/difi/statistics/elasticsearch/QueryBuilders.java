package no.difi.statistics.elasticsearch;

import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.BucketOrder.key;

public class QueryBuilders {

    private static final String timestampField = "timestamp";

    public static TermsAggregationBuilder sumPerTimestampAggregation(String name, List<String> measurementIds) {
        return summarizeMeasurements(name, measurementIds, null);
    }

    public static TermsAggregationBuilder summarizeMeasurements(String name, List<String> measurementIds, String categoryKey) {
        return terms(name)
                .field(timestampField)
                .size(10_000)
                .order(key(true))
                .subAggregations(subAggregation(categoryKey, measurementIds));
    }

    private static AggregatorFactories.Builder subAggregation(String categoryKey, List<String> measurementIds) {
        if (categoryKey == null)
            return sumMeasurements(measurementIds);
        return AggregatorFactories.builder().addAggregator(
                terms("perCategory:" + categoryKey)
                        .field("category." + categoryKey + ".keyword")
                        .size(10_000)
                        .order(key(true))
                        .subAggregations(sumMeasurements(measurementIds))
        );
    }

    private static AggregatorFactories.Builder sumMeasurements(List<String> measurementIds) {
        AggregatorFactories.Builder builder = AggregatorFactories.builder();
        measurementIds.forEach(measurementId -> builder.addAggregator(sum(measurementId).field(measurementId)));
        return builder;
    }

}
