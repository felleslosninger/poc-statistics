package no.difi.statistics.query.elasticsearch;

import com.google.common.collect.ImmutableMap;
import no.difi.statistics.elasticsearch.ResultParser;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.RelationalOperator;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.QueryService;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.lastAggregation;
import static no.difi.statistics.elasticsearch.QueryBuilders.lastHistogramAggregation;
import static no.difi.statistics.elasticsearch.QueryBuilders.sumAggregation;
import static no.difi.statistics.elasticsearch.QueryBuilders.sumHistogramAggregation;
import static no.difi.statistics.elasticsearch.QueryBuilders.timeRangeQuery;
import static no.difi.statistics.model.MeasurementDistance.days;
import static no.difi.statistics.model.MeasurementDistance.months;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;

public class ElasticsearchQueryService implements QueryService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String timeFieldName = "timestamp";
    private static final String indexType = "default";

    private Client elasticSearchClient;
    private ListAvailableTimeSeries.Command listAvailableTimeSeriesCommand;

    public ElasticsearchQueryService(Client elasticSearchClient, ListAvailableTimeSeries.Command listAvailableTimeSeriesCommand) {
        this.elasticSearchClient = elasticSearchClient;
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .addAggregation(lastAggregation())
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .setSize(10_000) // 10 000 is maximum
                .execute().actionGet();
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        for (SearchHit hit : response.getHits()) {
            series.add(ResultParser.point(hit));
        }
        return series;
    }

    private TimeSeriesPoint sumAggregate(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        if (from == null && to == null)
            return sumAggregateUnbounded(indexNames);
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .addAggregation(sumAggregation("a", from, to, measurementIds(indexNames)))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        if (response.getAggregations() == null)
            return null;
        return ResultParser.sumPointFromRangeBucket(response.getAggregations().get("a"));
    }

    private TimeSeriesPoint sumAggregateUnbounded(List<String> indexNames) {
        SearchRequestBuilder searchBuilder = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(null, null))
                .setSize(0); // We are after aggregation and not the search hits
        measurementIds(indexNames).forEach(mid -> searchBuilder.addAggregation(AggregationBuilders.sum(mid).field(mid)));
        searchBuilder.addAggregation(lastAggregation());
        SearchResponse response = searchBuilder.execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .addAggregation(sumHistogramAggregation("a", targetDistance, measurementIds(indexNames)))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .addAggregation(lastHistogramAggregation("a", targetDistance, measurementIds(indexNames)))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .setPostFilter(range(filter.getMeasurementId(), filter.getRelationalOperator(), percentileValue))
                .setSize(10_000) // 10 000 is maximum
                .execute().actionGet();
        List<TimeSeriesPoint> series = new ArrayList<>();
        logger.info("Search result:\n" + response);
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .setSize(0) // We are after aggregation and not the search hits
                .addAggregation(percentiles("p").field(measurementId).percentiles(percentile).compression(10000))
                .execute().actionGet();
        if (response.getAggregations() == null)
            return 0.0;
        return ((Percentiles)response.getAggregations().get("p")).percentile(percentile);
    }

    private SearchRequestBuilder searchBuilder(List<String> indexNames) {
        return elasticSearchClient
                .prepareSearch(indexNames.toArray(new String[indexNames.size()]))
                .addSort(timeFieldName, SortOrder.ASC)
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .setTypes(indexType);
    }

    private List<String> measurementIds(List<String> indexNames) {
        Map<String, GetFieldMappingsResponse.FieldMappingMetaData> fieldMapping =
                elasticSearchClient.admin().indices()
                        .prepareGetFieldMappings(indexNames.toArray(new String[indexNames.size()]))
                        .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                        .addTypes(indexType)
                        .setFields("*")
                        .get().mappings().entrySet().stream().findFirst().map(m -> m.getValue().get(indexType)).orElse(ImmutableMap.of());
        return fieldMapping.keySet().stream().filter(f -> !f.equals("timestamp") && !f.startsWith("_")).collect(toList());
    }


}
