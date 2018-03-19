package no.difi.statistics.query.elasticsearch;

import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.elasticsearch.IndexNameResolver;
import no.difi.statistics.elasticsearch.ResultParser;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.RelationalOperator;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.PercentileFilter;
import no.difi.statistics.model.query.QueryFilter;
import no.difi.statistics.query.QueryService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.*;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.*;
import static no.difi.statistics.model.MeasurementDistance.*;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.elasticsearch.search.sort.SortOrder.ASC;

public class ElasticsearchQueryService implements QueryService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String timeFieldName = "timestamp";
    private static final String indexType = "default";

    private Client elasticsearchClient;
    private ListAvailableTimeSeries.Command listAvailableTimeSeriesCommand;

    public ElasticsearchQueryService(Client elasticsearchClient, ListAvailableTimeSeries.Command listAvailableTimeSeriesCommand) {
        this.elasticsearchClient = elasticsearchClient;
        this.listAvailableTimeSeriesCommand = listAvailableTimeSeriesCommand;
    }

    @Override
    public List<TimeSeriesDefinition> availableTimeSeries() {
        return listAvailableTimeSeriesCommand.execute();
    }

    @Override
    public List<TimeSeriesPoint> query(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter) {
        List<TimeSeriesPoint> result = search(
                resolveIndexName().seriesDefinition(seriesDefinition).from(queryFilter.from()).to(queryFilter.to()).list(),
                queryFilter
        );
        if (result.isEmpty() && seriesDefinition.getDistance().equals(days)) {
            logger.info("Empty result for day series search. Attempting to aggregate minute series...");
            seriesDefinition = TimeSeriesDefinition.builder().name(seriesDefinition.getName()).distance(minutes).owner(seriesDefinition.getOwner());
            result = sumPerDistance(
                    resolveIndexName().seriesDefinition(seriesDefinition).from(queryFilter.from()).to(queryFilter.to()).list(),
                    days,
                    queryFilter
            );
        } else if (result.isEmpty() && seriesDefinition.getDistance().equals(months)) {
            logger.info("Empty result for month series search. Attempting to aggregate minute series...");
            seriesDefinition = TimeSeriesDefinition.builder().name(seriesDefinition.getName()).distance(minutes).owner(seriesDefinition.getOwner());
            result = sumPerDistance(
                    resolveIndexName().seriesDefinition(seriesDefinition).from(queryFilter.from()).to(queryFilter.to()).list(),
                    days,
                    queryFilter
            );
        }
        return result;
    }

    @Override
    public List<TimeSeriesPoint> query(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter, PercentileFilter filter) {
        return searchWithPercentileFilter(
                resolveIndexName().seriesDefinition(seriesDefinition).from(queryFilter.from()).to(queryFilter.to()).list(),
                queryFilter, filter
        );
    }

    @Override
    public List<TimeSeriesPoint> lastPerDistance(
            TimeSeriesDefinition seriesDefinition,
            MeasurementDistance targetDistance,
            QueryFilter queryFilter
    ){
        return lastPerDistance(
                resolveIndexName().seriesDefinition(seriesDefinition).from(queryFilter.from()).to(queryFilter.to()).list(),
                targetDistance,
                queryFilter
        );
    }

    @Override
    public TimeSeriesPoint sum(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter) {
        return sumAggregate(
                resolveIndexName().seriesDefinition(seriesDefinition).from(queryFilter.from()).to(queryFilter.to()).list(),
                queryFilter
        );
    }

    @Override
    public List<TimeSeriesPoint> sumPerDistance(TimeSeriesDefinition seriesDefinition, MeasurementDistance targetDistance, QueryFilter queryFilter) {
        return sumPerDistance(
                resolveIndexName().seriesDefinition(seriesDefinition).from(queryFilter.from()).to(queryFilter.to()).list(),
                targetDistance,
                queryFilter
        );
    }

    @Override
    public TimeSeriesPoint last(TimeSeriesDefinition seriesDefinition, QueryFilter queryFilter) {
        return last(
                resolveIndexName().seriesDefinition(seriesDefinition).from(queryFilter.from()).to(queryFilter.to()).list(),
                queryFilter
        );
    }

    private TimeSeriesPoint last(List<String> indexNames, QueryFilter queryFilter) {
        SearchResponse response = search(indexNames, searchSource(queryFilter)
                .aggregation(lastAggregation(measurementIds(indexNames)))
                .size(0) // We are after aggregation and not the search hits
        );
        return ResultParser.pointFromLastAggregation(response, queryFilter.categories());
    }

    private List<TimeSeriesPoint> search(List<String> indexNames, QueryFilter queryFilter) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing search:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    indexType,
                    queryFilter.from(),
                    queryFilter.to()
            ));
        }
        SearchResponse response = search(indexNames, searchSource(queryFilter)
                .aggregation(sumPerTimestampAggregation("categoryAggregation", measurementIds(indexNames)))
                .size(10_000) // 10 000 is maximum
        );
        if (response.getAggregations() != null)
            return ResultParser.points(response.getAggregations().get("categoryAggregation"), queryFilter.categories());
        else
            return emptyList();
    }

    private TimeSeriesPoint sumAggregate(List<String> indexNames, QueryFilter queryFilter) {
        if (queryFilter.from() == null && queryFilter.to() == null)
            return sumAggregateUnbounded(indexNames, queryFilter);
        SearchResponse response = search(indexNames, searchSource(queryFilter)
                .aggregation(sumAggregation("a", queryFilter.from(), queryFilter.to(), measurementIds(indexNames)))
                .size(0) // We are after aggregation and not the search hits
        );
        if (response.getAggregations() == null)
            return null;
        return ResultParser.sumPointFromRangeBucket(response.getAggregations().get("a")).categories(queryFilter.categories()).build();
    }

    private TimeSeriesPoint sumAggregateUnbounded(List<String> indexNames, QueryFilter queryFilter) {
        SearchSourceBuilder searchSourceBuilder = searchSource(queryFilter)
                .size(0); // We are after aggregation and not the search hits
        List<String> measurementIds = measurementIds(indexNames);
        measurementIds.forEach(mid -> searchSourceBuilder.aggregation(AggregationBuilders.sum(mid).field(mid)));
        searchSourceBuilder.aggregation(lastAggregation(measurementIds));
        SearchResponse response = search(indexNames, searchSourceBuilder);
        return ResultParser.sumPoint(response.getAggregations()).categories(queryFilter.categories()).build();
    }

    private List<TimeSeriesPoint> sumPerDistance(List<String> indexNames, MeasurementDistance targetDistance, QueryFilter queryFilter) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing sum point per %s:\nIndexes: %s\nFrom: %s\nTo: %s\n",
                    targetDistance,
                    indexNames.stream().collect(joining(",\n  ")),
                    queryFilter.from(),
                    queryFilter.to()
            ));
        }
        SearchSourceBuilder searchSource = searchSource(queryFilter)
                .aggregation(sumPerDistanceAggregation("a", targetDistance, measurementIds(indexNames)))
                .size(0); // We are after aggregation and not the search hits
        SearchResponse response = search(indexNames, searchSource);
        if (response.getAggregations() != null)
            return ResultParser.points(response.getAggregations().get("a"), queryFilter.categories());
        else
            return emptyList();
    }

    private List<TimeSeriesPoint> lastPerDistance(List<String> indexNames, MeasurementDistance targetDistance, QueryFilter queryFilter) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing last point per %s:\nIndexes: %s\nFrom: %s\nTo: %s\n",
                    targetDistance,
                    indexNames.stream().collect(joining(",\n  ")),
                    queryFilter.from(),
                    queryFilter.to()
            ));
        }
        SearchResponse response = search(indexNames, searchSource(queryFilter)
                .aggregation(lastPerDistanceAggregation("a", targetDistance, measurementIds(indexNames)))
                .size(0) // We are after aggregation and not the search hits
        );
        if (response.getAggregations() != null)
            return ResultParser.points(response.getAggregations().get("a"), queryFilter.categories());
        else
            return emptyList();
    }

    private List<TimeSeriesPoint> searchWithPercentileFilter(List<String> indexNames, QueryFilter queryFilter, PercentileFilter filter) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing search:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    indexType,
                    queryFilter.from(),
                    queryFilter.to()
            ));
        }
        double percentileValue = percentileValue(indexNames, filter.getMeasurementId(), filter.getPercentile(), queryFilter);
        logger.info(filter.getPercentile() + ". percentile value: " + percentileValue);
        SearchResponse response = search(indexNames, searchSource(queryFilter)
                .postFilter(range(filter.getMeasurementId(), filter.getRelationalOperator(), percentileValue))
                .size(10_000) // 10 000 is maximum
        );
        List<TimeSeriesPoint> series = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            series.add(ResultParser.point(hit).build());
        }
        return series;
    }

    private RangeQueryBuilder range(String measurementId, RelationalOperator operator, double percentileValue) {
        RangeQueryBuilder builder = rangeQuery(measurementId);
        switch (operator) {
            case gt:
            case gte:
                builder.gt((long)percentileValue);
                break;
            case lt:
            case lte:
                builder.lt((long)percentileValue + 1);
                break;
            default: throw new IllegalArgumentException(operator.toString());
        }
        return builder;
    }

    private double percentileValue(List<String> indexNames, String measurementId, int percentile, QueryFilter queryFilter) {
        SearchResponse response = search(indexNames, searchSource(queryFilter)
                        .size(0) // We are after aggregation and not the search hits
                        .aggregation(percentiles("p").field(measurementId).percentiles(percentile).compression(10000))
        );
        if (response.getAggregations() == null)
            return 0.0;
        return ((Percentiles)response.getAggregations().get("p")).percentile(percentile);
    }

    private SearchSourceBuilder searchSource(QueryFilter queryFilter) {
        BoolQueryBuilder boolQuery = boolQuery();
        boolQuery.filter(timeRangeQuery(queryFilter.from(), queryFilter.to()));
        queryFilter.categories().forEach((k, v) -> boolQuery.filter(categoryQuery(k, v)));
        return SearchSourceBuilder.searchSource().query(boolQuery);
    }

    private SearchResponse search(List<String> indexNames, SearchSourceBuilder searchSource) {
        try {
            return elasticsearchClient.highLevel().search(searchRequest(indexNames).source(searchSource.sort(timeFieldName, ASC)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to search", e);
        }
    }

    private SearchRequest searchRequest(List<String> indexNames) {
        return new SearchRequest(indexNames.toArray(new String[indexNames.size()]))
                .indicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .types(indexType);
    }

    private List<String> measurementIds(List<String> indexNames) {
        String genericIndexName = IndexNameResolver.generic(indexNames.get(0));
        Set<String> result = new HashSet<>();
        try (InputStream response = elasticsearchClient.lowLevel()
                .performRequest("GET", "/" + genericIndexName + "/_mappings?ignore_unavailable=true").getEntity().getContent()) {
            JsonReader reader = Json.createReader(response);
            reader.readObject().forEach(
                    (key, value) -> result.addAll(
                            value.asJsonObject().getJsonObject("mappings").getJsonObject("default")
                                    .getJsonObject("properties").keySet().stream()
                                    .filter(p -> !p.startsWith("category."))
                                    .filter(p -> !p.equals("category"))
                                    .filter(p -> !p.equals(timeFieldName))
                                    .collect(toSet())
                    )
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to get available measurement ids", e);
        }
        return new ArrayList<>(result);
    }


}
