package no.difi.statistics.query.elasticsearch.commands;

import no.difi.statistics.model.RelationalOperator;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.PercentileFilter;
import no.difi.statistics.query.model.QueryFilter;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;

public class PercentileQuery extends Query {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String timeFieldName = "timestamp";
    private static final String indexType = "default";
    private TimeSeriesDefinition seriesDefinition;
    private QueryFilter queryFilter;
    private PercentileFilter percentileFilter;

    public List<TimeSeriesPoint> execute() {
        return searchWithPercentileFilter(
                resolveIndexName().seriesDefinition(seriesDefinition).range(queryFilter.timeRange()).list(),
                queryFilter, percentileFilter
        );
    }

    private List<TimeSeriesPoint> searchWithPercentileFilter(List<String> indexNames, QueryFilter queryFilter, PercentileFilter filter) {
        double percentileValue = percentileValue(indexNames, filter.getMeasurementId(), filter.getPercentile(), queryFilter);
        logger.info(filter.getPercentile() + ". percentile value: " + percentileValue);
        SearchResponse response = search(searchRequest(
                indexNames,
                queryFilter,
                range(filter.getMeasurementId(), filter.getRelationalOperator(), percentileValue),
                10_000
        ));
        List<TimeSeriesPoint> series = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            series.add(point(hit).build());
        }
        return series;
    }

    private static TimeSeriesPoint.Builder point(SearchHit hit) {
        return TimeSeriesPoint.builder()
                .timestamp(timestamp(hit))
                .measurements(measurements(hit))
                .categories(categories(hit));
    }

    private static Map<String, Long> measurements(SearchHit hit) {
        return hit.getSourceAsMap().entrySet().stream()
                .filter(entry -> !entry.getKey().equals(timeFieldName) && !entry.getKey().startsWith("category."))
                .collect(toMap(Map.Entry::getKey, e -> Long.valueOf(e.getValue().toString())));
    }

    private static Map<String, String> categories(SearchHit hit) {
        return hit.getSourceAsMap().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("category."))
                .collect(toMap(entry -> entry.getKey().substring(9), entry -> entry.getValue().toString()));
    }

    private RangeQueryBuilder range(String measurementId, RelationalOperator operator, double percentileValue) {
        RangeQueryBuilder builder = rangeQuery(measurementId);
        switch (operator) {
            case gt:
            case gte:
                builder.gt((long) percentileValue);
                break;
            case lt:
            case lte:
                builder.lt((long) percentileValue + 1);
                break;
            default:
                throw new IllegalArgumentException(operator.toString());
        }
        return builder;
    }

    private double percentileValue(List<String> indexNames, String measurementId, int percentile, QueryFilter queryFilter) {
        SearchResponse response = search(searchRequest(
                indexNames,
                queryFilter,
                null,
                0,
                percentiles("p").field(measurementId).percentiles(percentile).compression(10000)
        ));
        if (response.getAggregations() == null)
            return 0.0;
        return ((Percentiles) response.getAggregations().get("p")).percentile(percentile);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private PercentileQuery instance = new PercentileQuery();

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

        public Builder percentileFilter(PercentileFilter percentileFilter) {
            instance.percentileFilter = percentileFilter;
            return this;
        }

        public PercentileQuery build() {
            return instance;
        }

    }

}
