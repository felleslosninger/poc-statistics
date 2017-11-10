package no.difi.statistics.query.elasticsearch;

import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.elasticsearch.ResultParser;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.RelationalOperator;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.QueryService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.*;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.*;
import static no.difi.statistics.model.MeasurementDistance.days;
import static no.difi.statistics.model.MeasurementDistance.months;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
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
    public List<TimeSeriesPoint> minutes(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveIndexName().seriesName(seriesName).owner(owner).minutes().from(from).to(to).list(), from, to);
    }

    @Override
    public List<TimeSeriesPoint> query(String seriesName, MeasurementDistance distance, String owner, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter) {
        return searchWithPercentileFilter(
                resolveIndexName().seriesName(seriesName).owner(owner).distance(distance).from(from).to(to).list(),
                from, to, filter
        );
    }

    @Override
    public List<TimeSeriesPoint> hours(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to) {
        return search(
                resolveIndexName().seriesName(seriesName).owner(owner).hours().from(from).to(to).list(),
                from, to
        );
    }

    @Override
    public List<TimeSeriesPoint> days(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to) {
        List<TimeSeriesPoint> result = search(
                resolveIndexName().seriesName(seriesName).owner(owner).days().from(from).to(to).list(),
                from, to
        );
        if (result.isEmpty()) {
            logger.info("Empty result for day series search. Attempting to aggregate minute series...");
            result = sumPerDistance(
                    resolveIndexName().seriesName(seriesName).owner(owner).minutes().from(from).to(to).list(),
                    days,
                    from, to
            );
        }
        return result;
    }

    @Override
    public List<TimeSeriesPoint> months(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to) {
        List<TimeSeriesPoint> result = search(
                resolveIndexName().seriesName(seriesName).owner(owner).months().from(from).to(to).list(),
                from, to
        );
        if (result.isEmpty()) {
            logger.info("Empty result for month series search. Attempting to aggregate minute series...");
            result = sumPerDistance(
                    resolveIndexName().seriesName(seriesName).owner(owner).minutes().from(from).to(to).list(),
                    months,
                    from, to
            );
        }
        return result;
    }

    @Override
    public List<TimeSeriesPoint> lastPerDistance(
            String seriesName,
            MeasurementDistance distance,
            MeasurementDistance targetDistance,
            String owner,
            ZonedDateTime from,
            ZonedDateTime to
    ){
        return lastPerDistance(
                resolveIndexName().seriesName(seriesName).owner(owner).distance(distance).from(from).to(to).list(),
                targetDistance,
                from,
                to);
    }

    @Override
    public List<TimeSeriesPoint> years(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to) {
        return search(
                resolveIndexName().seriesName(seriesName).owner(owner).years().from(from).to(to).list(),
                from, to
        );
    }

    @Override
    public TimeSeriesPoint sum(String seriesName, MeasurementDistance distance, String owner, ZonedDateTime from, ZonedDateTime to) {
        return sumAggregate(
                resolveIndexName().seriesName(seriesName).owner(owner).distance(distance).from(from).to(to).list(),
                from, to
        );
    }

    @Override
    public List<TimeSeriesPoint> sumPerDistance(String seriesName, MeasurementDistance distance, MeasurementDistance targetDistance, String owner, ZonedDateTime from, ZonedDateTime to) {
        return sumPerDistance(
                resolveIndexName().seriesName(seriesName).owner(owner).distance(distance).from(from).to(to).list(),
                targetDistance,
                from,
                to);
    }

    @Override
    public TimeSeriesPoint last(String seriesName, MeasurementDistance distance, String owner, ZonedDateTime from, ZonedDateTime to) {
        return last(
                resolveIndexName().seriesName(seriesName).owner(owner).distance(distance).from(from).to(to).list(),
                from, to
        );
    }

    private TimeSeriesPoint last(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        SearchResponse response = search(indexNames, searchSource()
                .query(timeRangeQuery(from, to))
                .aggregation(lastAggregation())
                .size(0) // We are after aggregation and not the search hits
        );
        return ResultParser.pointFromLastAggregation(response);
    }

    private List<TimeSeriesPoint> search(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing search:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    indexType,
                    from,
                    to
            ));
        }
        SearchResponse response = search(indexNames, searchSource()
                .query(timeRangeQuery(from, to))
                .size(10_000) // 10 000 is maximum
        );
        List<TimeSeriesPoint> series = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            series.add(ResultParser.point(hit));
        }
        return series;
    }

    private TimeSeriesPoint sumAggregate(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        if (from == null && to == null)
            return sumAggregateUnbounded(indexNames);
        SearchResponse response = search(indexNames, searchSource()
                .query(timeRangeQuery(from, to))
                .aggregation(sumAggregation("a", from, to, measurementIds(indexNames)))
                .size(0) // We are after aggregation and not the search hits
        );
        if (response.getAggregations() == null)
            return null;
        return ResultParser.sumPointFromRangeBucket(response.getAggregations().get("a"));
    }

    private TimeSeriesPoint sumAggregateUnbounded(List<String> indexNames) {
        SearchSourceBuilder searchSourceBuilder = searchSource()
                .query(timeRangeQuery(null, null))
                .size(0); // We are after aggregation and not the search hits
        measurementIds(indexNames).forEach(mid -> searchSourceBuilder.aggregation(AggregationBuilders.sum(mid).field(mid)));
        searchSourceBuilder.aggregation(lastAggregation());
        SearchResponse response = search(indexNames, searchSourceBuilder);
        return ResultParser.sumPoint(response.getAggregations());
    }

    private List<TimeSeriesPoint> sumPerDistance(List<String> indexNames, MeasurementDistance targetDistance, ZonedDateTime from, ZonedDateTime to) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing sum point per %s:\nIndexes: %s\nFrom: %s\nTo: %s\n",
                    targetDistance,
                    indexNames.stream().collect(joining(",\n  ")),
                    from,
                    to
            ));
        }
        SearchResponse response = search(indexNames, searchSource()
                .query(timeRangeQuery(from, to))
                .aggregation(sumHistogramAggregation("a", targetDistance, measurementIds(indexNames)))
                .size(0) // We are after aggregation and not the search hits
        );
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (response.getAggregations() != null) {
            Histogram histogram = response.getAggregations().get("a");
            series.addAll(histogram.getBuckets().stream().map(ResultParser::point).collect(toList()));
        }
        return series;
    }

    private List<TimeSeriesPoint> lastPerDistance(List<String> indexNames, MeasurementDistance targetDistance, ZonedDateTime from, ZonedDateTime to) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing last point per %s:\nIndexes: %s\nFrom: %s\nTo: %s\n",
                    targetDistance,
                    indexNames.stream().collect(joining(",\n  ")),
                    from,
                    to
            ));
        }
        SearchResponse response = search(indexNames, searchSource()
                .query(timeRangeQuery(from, to))
                .aggregation(lastHistogramAggregation("a", targetDistance, measurementIds(indexNames)))
                .size(0) // We are after aggregation and not the search hits
        );
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (response.getAggregations() != null) {
            Histogram histogram = response.getAggregations().get("a");
            series.addAll(histogram.getBuckets().stream().map(ResultParser::point).collect(toList()));
        }
        return series;
    }

    private List<TimeSeriesPoint> searchWithPercentileFilter(List<String> indexNames, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing search:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    indexType,
                    from,
                    to
            ));
        }
        double percentileValue = percentileValue(indexNames, filter.getMeasurementId(), filter.getPercentile(), from, to);
        logger.info(filter.getPercentile() + ". percentile value: " + percentileValue);
        SearchResponse response = search(indexNames, searchSource()
                .query(timeRangeQuery(from, to))
                .postFilter(range(filter.getMeasurementId(), filter.getRelationalOperator(), percentileValue))
                .size(10_000) // 10 000 is maximum
        );
        List<TimeSeriesPoint> series = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            series.add(ResultParser.point(hit));
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

    private double percentileValue(List<String> indexNames, String measurementId, int percentile, ZonedDateTime from, ZonedDateTime to) {
        SearchResponse response = search(indexNames, searchSource()
                        .query(timeRangeQuery(from, to))
                        .size(0) // We are after aggregation and not the search hits
                        .aggregation(percentiles("p").field(measurementId).percentiles(percentile).compression(10000))
        );
        if (response.getAggregations() == null)
            return 0.0;
        return ((Percentiles)response.getAggregations().get("p")).percentile(percentile);
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
        Set<String> result = new HashSet<>();
        try (InputStream response = elasticsearchClient.lowLevel()
                .performRequest("GET", "/" + join(",", indexNames) + "/_mappings?ignore_unavailable=true").getEntity().getContent()) {
            JsonReader reader = Json.createReader(response);
            reader.readObject().forEach(
                    (key, value) -> result.addAll(
                            value.asJsonObject().getJsonObject("mappings").getJsonObject("default")
                                    .getJsonObject("properties").keySet().stream()
                                    .filter(p -> !p.equals(timeFieldName)).collect(toSet())
                    )
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to get available measurement ids", e);
        }
        return new ArrayList<>(result);
    }


}
