package no.difi.statistics.api;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static no.difi.statistics.util.IndexNameResolver.resolveDayIndexNames;
import static no.difi.statistics.util.IndexNameResolver.resolveHourIndexNames;
import static no.difi.statistics.util.IndexNameResolver.resolveMinuteIndexNames;
import static no.difi.statistics.util.IndexNameResolver.resolveMonthIndexNames;
import static no.difi.statistics.util.IndexNameResolver.resolveYearIndexNames;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;

public class Statistics {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String timeFieldName = "timestamp";

    private Client elasticSearchClient;
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public Statistics(Client elasticSearchClient) {
        this.elasticSearchClient = elasticSearchClient;
    }

    public List<TimeSeriesPoint> minutes(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveMinuteIndexNames(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> minutes(String seriesName, String type, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter) {
        return searchWithPercentileFilter(resolveMinuteIndexNames(seriesName, from, to), type, from, to, filter);
    }

    public List<TimeSeriesPoint> hours(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveHourIndexNames(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> days(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveDayIndexNames(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> months(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        List<TimeSeriesPoint> result = search(resolveMonthIndexNames(seriesName, from, to), type, from, to);
        if (result.isEmpty()) {
            logger.info("Empty result for month series search. Attempting to aggregate minute series...");
            result = sumAggregatePerMonth(resolveMinuteIndexNames(seriesName, from, to), type, from, to);
        }
        return result;
    }

    public List<TimeSeriesPoint> years(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveYearIndexNames(seriesName, from, to), type, from, to);
    }

    private List<TimeSeriesPoint> search(List<String> indexNames, String type, ZonedDateTime from, ZonedDateTime to) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing search:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    type,
                    from,
                    to
            ));
        }
        SearchResponse response = elasticSearchClient
                .prepareSearch(indexNames.stream().collect(joining(",")))
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .setTypes(type)
                .setQuery(timeRange(from, to))
                .addSort(timeFieldName, SortOrder.ASC)
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

    private List<TimeSeriesPoint> sumAggregatePerMonth(List<String> indexNames, String type, ZonedDateTime from, ZonedDateTime to) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing sum aggregate for per month:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    type,
                    from,
                    to
            ));
        }
        SearchResponse response = elasticSearchClient
                .prepareSearch(indexNames.stream().collect(joining(",")))
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .setTypes(type)
                .setQuery(timeRange(from, to))
                .addSort(timeFieldName, SortOrder.ASC)
                .addAggregation(
                        dateHistogram("per_month").field("timestamp").interval(DateHistogramInterval.MONTH)
                        .subAggregation(sum("count").field("count")) // TODO: Need sub-(sum-)aggregation per (integral) field in source documents
                )
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        Histogram histogram = response.getAggregations().get("per_month");
        for (Histogram.Bucket bucket : histogram.getBuckets()) {
            series.add(point(bucket));
        }
        return series;
    }

    private List<TimeSeriesPoint> searchWithPercentileFilter(List<String> indexNames, String type, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing search:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    type,
                    from,
                    to
            ));
        }
        SearchResponse response = elasticSearchClient
                .prepareSearch(indexNames.stream().collect(joining(",")))
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .setTypes(type)
                .setQuery(timeRange(from, to))
                .addSort(timeFieldName, SortOrder.ASC)
                .setSize(0) // We are after aggregation and not the search hits
                .addAggregation(percentiles("idporten_login_percentiles").field(filter.getMeasurementId()).percentiles(filter.getPercentile()))
                .execute().actionGet();
        double percentileValue = ((Percentiles)response.getAggregations().get("idporten_login_percentiles")).percentile(filter.getPercentile());
        logger.info(filter.getPercentile() + ". percentile value: " + percentileValue);
        response = elasticSearchClient
                .prepareSearch(indexNames.stream().collect(joining(",")))
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .setTypes(type)
                .setQuery(timeRange(from, to))
                .setPostFilter(QueryBuilders.rangeQuery(filter.getMeasurementId()).gt(percentileValue))
                .addSort(timeFieldName, SortOrder.ASC)
                .setSize(10_000) // 10 000 is maximum
                .execute().actionGet();
        List<TimeSeriesPoint> series = new ArrayList<>();
        logger.info("Search result:\n" + response);
        for (SearchHit hit : response.getHits()) {
            series.add(point(hit));
        }
        return series;
    }

    private RangeQueryBuilder timeRange(ZonedDateTime from, ZonedDateTime to) {
        return QueryBuilders.rangeQuery(timeFieldName).from(dateTimeFormatter.format(from)).to(dateTimeFormatter.format(to));
    }

    private TimeSeriesPoint point(SearchHit hit) {
        return TimeSeriesPoint.builder().timestamp(time(hit)).measurements(measurements(hit)).build();
    }

    private TimeSeriesPoint point(Histogram.Bucket bucket) {
        return TimeSeriesPoint.builder().timestamp(time(bucket.getKeyAsString())).measurements(measurements(bucket)).build();
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

    private List<Measurement> measurements(Histogram.Bucket bucket) {
        List<Measurement> measurements = new ArrayList<>();
        for (Aggregation sum : bucket.getAggregations()) {
            measurements.add(new Measurement(sum.getName(), (int)((Sum)sum).getValue()));
        }
        return measurements;
    }

}
