package no.difi.statistics.query.elasticsearch;

import com.google.common.collect.ImmutableMap;
import no.difi.statistics.elasticsearch.ResultParser;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.QueryService;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;

public class ElasticsearchQueryService implements QueryService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String timeFieldName = "timestamp";
    private static final String indexType = "default";

    private Client elasticSearchClient;

    public ElasticsearchQueryService(Client elasticSearchClient) {
        this.elasticSearchClient = elasticSearchClient;
    }

    public List<String> availableTimeSeries(String owner) {

        final String pattern = owner + ":(.*):minute.*";
        String[] timeseries = elasticSearchClient.admin().cluster()
                .prepareState().execute()
                .actionGet().getState()
                .getMetaData().concreteAllIndices();

        Set<String> seriesNames = new HashSet<>();
        Pattern p = Pattern.compile(pattern);
        for (String serie : timeseries) {
            Matcher m = p.matcher(serie);
            if(m.find())
            seriesNames.add(m.group(1));
        }

        List<String> availableTimeSeries = new ArrayList<>();
        availableTimeSeries.addAll(seriesNames);
        Collections.sort(availableTimeSeries);

        return availableTimeSeries;
    }

    @Override
    public List<TimeSeriesPoint> minutes(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveIndexName().seriesName(seriesName).owner(owner).minutes().from(from).to(to).list(), from, to);
    }

    @Override
    public List<TimeSeriesPoint> minutes(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter) {
        return searchWithPercentileFilter(
                resolveIndexName().seriesName(seriesName).owner(owner).minutes().from(from).to(to).list(),
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
            result = sumAggregatePerDay(
                    resolveIndexName().seriesName(seriesName).owner(owner).minutes().from(from).to(to).list(),
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
            result = sumAggregatePerMonth(
                    resolveIndexName().seriesName(seriesName).owner(owner).minutes().from(from).to(to).list(),
                    from, to
            );
        }
        return result;
    }

    @Override
    public List<TimeSeriesPoint> lastInMonths(String seriesName, String owner, ZonedDateTime from , ZonedDateTime to){
        List<TimeSeriesPoint> result = search(
                resolveIndexName().seriesName(seriesName).owner(owner).months().from(from).to(to).list(),
                from, to
        );
        if (result.isEmpty()) {
            logger.info("Empty result for month series search. Attempting to aggregate minute series...");
            result = lastInMonth(
                    resolveIndexName().seriesName(seriesName).owner(owner).minutes().from(from).to(to).list(),
                    from, to
            );
        }
        return result;
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
    public TimeSeriesPoint last(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to) {
        return last(
                resolveIndexName().seriesName(seriesName).owner(owner).minutes().from(from).to(to).list(),
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .addAggregation(dateRangeAggregation("a", from, to, measurementIds(indexNames)))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        if (response.getAggregations() == null)
            return null;
        Range range = response.getAggregations().get("a");
        return ResultParser.point(range.getBuckets().get(0));
    }

    private List<TimeSeriesPoint> sumAggregatePerMonth(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing sum aggregate per month:\nIndexes: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    from,
                    to
            ));
        }
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .addAggregation(sumHistogramAggregation("per_month", DateHistogramInterval.MONTH, measurementIds(indexNames)))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (response.getAggregations() != null) {
            Histogram histogram = response.getAggregations().get("per_month");
            series.addAll(histogram.getBuckets().stream().map(ResultParser::point).collect(toList()));
        }
        return series;
    }

    private List<TimeSeriesPoint> sumAggregatePerDay(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing sum aggregate per day:\nIndexes: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    from,
                    to
            ));
        }
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .addAggregation(sumHistogramAggregation("per_day", DateHistogramInterval.DAY, measurementIds(indexNames)))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (response.getAggregations() != null) {
            Histogram histogram = response.getAggregations().get("per_day");
            series.addAll(histogram.getBuckets().stream().map(ResultParser::point).collect(toList()));
        }
        return series;
    }

    private List<TimeSeriesPoint> lastInMonth(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing last point per month:\nIndexes: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    from,
                    to
            ));
        }
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .addAggregation(lastHistogramAggregation("per_month", DateHistogramInterval.MONTH, measurementIds(indexNames)))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (response.getAggregations() != null) {
            Histogram histogram = response.getAggregations().get("per_month");
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
                .setPostFilter(rangeQuery(filter.getMeasurementId()).gt(percentileValue))
                .setSize(10_000) // 10 000 is maximum
                .execute().actionGet();
        List<TimeSeriesPoint> series = new ArrayList<>();
        logger.info("Search result:\n" + response);
        for (SearchHit hit : response.getHits()) {
            series.add(ResultParser.point(hit));
        }
        return series;
    }

    private double percentileValue(List<String> indexNames, String measurementId, int percentile, ZonedDateTime from, ZonedDateTime to) {
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .setSize(0) // We are after aggregation and not the search hits
                .addAggregation(percentiles("p").field(measurementId).percentiles(percentile).compression(10000))
                .execute().actionGet();
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
