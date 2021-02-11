package no.difi.statistics.query.elasticsearch.commands;

import no.difi.statistics.elasticsearch.Timestamp;
import no.difi.statistics.model.TimeRange;
import no.difi.statistics.query.model.QueryFilter;
import org.apache.http.ConnectionClosedException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.sort.SortOrder.ASC;

public abstract class Query {

    private static final String indexType = "default";
    private static final String timeFieldName = "timestamp";
    RestHighLevelClient elasticsearchClient;

    SearchResponse search(SearchRequest request) {
        try {
            return elasticsearchClient.search(request);
        } catch (IOException e) {
            try {
                return elasticsearchClient.search(request);
            } catch (IOException ee) {
                throw new RuntimeException("Search failed (performed a retry after IOException)", ee);
            }
        }
    }

    Map<String, Long> measurementsFromSumAggregations(Aggregations aggregations) {
        return aggregations.asList().stream().filter(a -> a instanceof Sum).map(a -> (Sum)a).collect(toMap(Aggregation::getName, a -> (long)a.getValue()));
    }

    protected static ZonedDateTime timestamp(SearchHit hit) {
        return Timestamp.parse(hit.getSourceAsMap().get(timeFieldName).toString());
    }

    static SearchRequest searchRequest(List<String> indexNames, QueryFilter queryFilter, QueryBuilder postFilter, int resultSize, AggregationBuilder...aggregations) {
        SearchSourceBuilder searchSource = searchSource(queryFilter)
                .postFilter(postFilter)
                .size(resultSize)
                .sort(timeFieldName, ASC);
        for (AggregationBuilder aggregation : aggregations)
            searchSource.aggregation(aggregation);
        return new SearchRequest(indexNames.toArray(new String[0]))
                .indicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .types(indexType)
                .source(searchSource);
    }

    private static SearchSourceBuilder searchSource(QueryFilter queryFilter) {
        BoolQueryBuilder boolQuery = boolQuery();
        if (queryFilter.timeRange() != null)
            boolQuery.filter(timeRangeQuery(queryFilter.timeRange()));
        queryFilter.categories().forEach((k, v) -> boolQuery.filter(categoryQuery(k, v)));
        return SearchSourceBuilder.searchSource().query(boolQuery);
    }

    private static MatchQueryBuilder categoryQuery(String key, String value) {
        return matchQuery("category." + key, value).operator(Operator.AND);
    }

    private static RangeQueryBuilder timeRangeQuery(TimeRange timeRange) {
        RangeQueryBuilder builder = rangeQuery(timeFieldName);
        if (timeRange.from() != null)
            builder.from(Timestamp.format(timeRange.from()));
        if (timeRange.to() != null)
            builder.to(Timestamp.format(timeRange.to()));
        return builder;
    }

}
