package no.difi.statistics.elasticsearch;

import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.BucketOrder.key;

public class QueryBuilders {

    private static final String timestampField = "timestamp";

    public static TermsAggregationBuilder sumPerTimestampAggregation(String name, List<String> measurementIds) {
        TermsAggregationBuilder builder = terms(name).field(timestampField).size(10_000).order(key(true));
        for (String measurementId : measurementIds)
            builder.subAggregation(sum(measurementId).field(measurementId));
        return builder;
    }

}
