package no.difi.statistics.query.elasticsearch;

import com.google.common.collect.ImmutableMap;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.QueryService;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;

public class ElasticsearchQueryService implements QueryService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String timeFieldName = "timestamp";
    private static final String defaultType = "default";

    private Client elasticSearchClient;
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public ElasticsearchQueryService(Client elasticSearchClient) {
        this.elasticSearchClient = elasticSearchClient;
    }

    @Override
    public List<TimeSeriesPoint> minutes(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveMinuteIndexNames(seriesName, from, to), from, to);
    }

    @Override
    public List<TimeSeriesPoint> minutes(String seriesName, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter) {
        return searchWithPercentileFilter(resolveMinuteIndexNames(seriesName, from, to), from, to, filter);
    }

    @Override
    public List<TimeSeriesPoint> hours(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveHourIndexNames(seriesName, from, to), from, to);
    }

    @Override
    public List<TimeSeriesPoint> days(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveDayIndexNames(seriesName, from, to), from, to);
    }

    @Override
    public List<TimeSeriesPoint> months(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        List<TimeSeriesPoint> result = search(resolveMonthIndexNames(seriesName, from, to), from, to);
        if (result.isEmpty()) {
            logger.info("Empty result for month series search. Attempting to aggregate minute series...");
            result = sumAggregatePerMonth(resolveMinuteIndexNames(seriesName, from, to), from, to);
        }
        return result;
    }

    @Override
    public List<TimeSeriesPoint> years(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveYearIndexNames(seriesName, from, to), from, to);
    }

    @Override
    public TimeSeriesPoint point(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return sumAggregate(resolveMinuteIndexNames(seriesName, from, to), from, to);
    }

    private List<TimeSeriesPoint> search(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing search:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    defaultType,
                    from,
                    to
            ));
        }
        SearchResponse response = searchBuilder(indexNames, from, to)
                .setSize(10_000) // 10 000 is maximum
                .execute().actionGet();
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        for (SearchHit hit : response.getHits()) {
            series.add(point(hit));
        }
        return series;
    }

    private TimeSeriesPoint sumAggregate(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        SearchResponse response = searchBuilder(indexNames, from, to)
                .addAggregation(dateRangeBuilder("a", from, to, measurementIds(indexNames)))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        if (response.getAggregations() == null)
            return null;
        Range range = response.getAggregations().get("a");
        return point(range.getBuckets().get(0));
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
        SearchResponse response = searchBuilder(indexNames, from, to)
                .addAggregation(dateHistogramBuilder("per_month", DateHistogramInterval.MONTH, measurementIds(indexNames)))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (response.getAggregations() != null) {
            Histogram histogram = response.getAggregations().get("per_month");
            series.addAll(histogram.getBuckets().stream().map(this::point).collect(toList()));
        }
        return series;
    }

    private List<TimeSeriesPoint> searchWithPercentileFilter(List<String> indexNames, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing search:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    defaultType,
                    from,
                    to
            ));
        }
        double percentileValue = percentileValue(indexNames, filter.getMeasurementId(), filter.getPercentile(), from, to);
        logger.info(filter.getPercentile() + ". percentile value: " + percentileValue);
        SearchResponse response = searchBuilder(indexNames, from, to)
                .setPostFilter(QueryBuilders.rangeQuery(filter.getMeasurementId()).gt(percentileValue))
                .setSize(10_000) // 10 000 is maximum
                .execute().actionGet();
        List<TimeSeriesPoint> series = new ArrayList<>();
        logger.info("Search result:\n" + response);
        for (SearchHit hit : response.getHits()) {
            series.add(point(hit));
        }
        return series;
    }

    private double percentileValue(List<String> indexNames, String measurementId, int percentile, ZonedDateTime from, ZonedDateTime to) {
        SearchResponse response = searchBuilder(indexNames, from, to)
                .setSize(0) // We are after aggregation and not the search hits
                .addAggregation(percentiles("p").field(measurementId).percentiles(percentile).compression(10000))
                .execute().actionGet();
        return ((Percentiles)response.getAggregations().get("p")).percentile(percentile);
    }

    private SearchRequestBuilder searchBuilder(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        return elasticSearchClient
                .prepareSearch(indexNames.toArray(new String[indexNames.size()]))
                .setQuery(timeRange(from, to))
                .addSort(timeFieldName, SortOrder.ASC)
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .setTypes(defaultType);
    }

    private List<String> measurementIds(List<String> indexNames) {
        Map<String, GetFieldMappingsResponse.FieldMappingMetaData> fieldMapping =
                elasticSearchClient.admin().indices()
                        .prepareGetFieldMappings(indexNames.toArray(new String[indexNames.size()]))
                        .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                        .addTypes(defaultType)
                        .setFields("*")
                        .get().mappings().entrySet().stream().findFirst().map(m -> m.getValue().get(defaultType)).orElse(ImmutableMap.of());
        return fieldMapping.keySet().stream().filter(f -> !f.equals("timestamp") && !f.startsWith("_")).collect(toList());
    }

    private DateHistogramBuilder dateHistogramBuilder(String name, DateHistogramInterval interval, List<String> measurementIds) {
        DateHistogramBuilder builder = AggregationBuilders.dateHistogram(name).field("timestamp").interval(interval);
        for (String measurementId : measurementIds)
            builder.subAggregation(AggregationBuilders.sum(measurementId).field(measurementId));
        return builder;
    }

    private DateRangeBuilder dateRangeBuilder(String name, ZonedDateTime from, ZonedDateTime to, List<String> measurementIds) {
        DateRangeBuilder builder = AggregationBuilders.dateRange(name).field(timeFieldName).addRange(formatTimestamp(from), formatTimestamp(to));
        for (String measurementId : measurementIds)
            builder.subAggregation(AggregationBuilders.sum(measurementId).field(measurementId));
        return builder;
    }

    private RangeQueryBuilder timeRange(ZonedDateTime from, ZonedDateTime to) {
        return QueryBuilders.rangeQuery(timeFieldName).from(dateTimeFormatter.format(from)).to(dateTimeFormatter.format(to));
    }

    private TimeSeriesPoint point(SearchHit hit) {
        return TimeSeriesPoint.builder().timestamp(time(hit)).measurements(measurements(hit)).build();
    }

    private TimeSeriesPoint point(Histogram.Bucket bucket) {
        return point(bucket.getKeyAsString(), bucket);
    }

    private TimeSeriesPoint point(Range.Bucket bucket) {
        return point(bucket.getFromAsString(), bucket);
    }

    private TimeSeriesPoint point(String timestamp, MultiBucketsAggregation.Bucket bucket) {
        return TimeSeriesPoint.builder().timestamp(time(timestamp)).measurements(measurements(bucket.getAggregations())).build();
    }

    private ZonedDateTime time(SearchHit hit) {
        return time(hit.getSource().get(timeFieldName).toString());
    }

    private ZonedDateTime time(String value) {
        return ZonedDateTime.parse(value, dateTimeFormatter);
    }

    private List<Measurement> measurements(SearchHit hit) {
        List<Measurement> measurements = new ArrayList<>();
        hit.getSource().keySet().stream().filter(field -> !field.equals(timeFieldName)).forEach(field -> {
            int value = Integer.valueOf(hit.getSource().get(field).toString());
            measurements.add(new Measurement(field, value));
        });
        return measurements;
    }

    private List<Measurement> measurements(Aggregations sumAggregations) {
        List<Measurement> measurements = new ArrayList<>();
        for (Aggregation sum : sumAggregations) {
            measurements.add(new Measurement(sum.getName(), (int)((Sum)sum).getValue()));
        }
        return measurements;
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return dateTimeFormatter.format(timestamp);
    }

}
