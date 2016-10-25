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
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;

public class ElasticsearchQueryService implements QueryService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String timeFieldName = "timestamp";
    private static final String defaultType = "default";

    private Client elasticSearchClient;
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

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
    public TimeSeriesPoint point(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to) {
        return sumAggregate(
                resolveIndexName().seriesName(seriesName).owner(owner).minutes().from(from).to(to).list(),
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
                .setQuery(timeRange(from, to))
                .addAggregation(topHits("last_updated").setSize(1).addSort("timestamp", SortOrder.DESC))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        TopHits topHits = response.getAggregations().get("last_updated");
        return point(topHits.getHits().getAt(0));
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRange(from, to))
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRange(from, to))
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRange(from, to))
                .addAggregation(sumHistogramBuilder("per_month", DateHistogramInterval.MONTH, measurementIds(indexNames)))
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
                .setQuery(timeRange(from, to))
                .addAggregation(sumHistogramBuilder("per_day", DateHistogramInterval.DAY, measurementIds(indexNames)))
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (response.getAggregations() != null) {
            Histogram histogram = response.getAggregations().get("per_day");
            series.addAll(histogram.getBuckets().stream().map(this::point).collect(toList()));
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
                .setQuery(timeRange(from, to))
                .addAggregation(lastHistogramBuilder("per_month", DateHistogramInterval.MONTH, measurementIds(indexNames)))
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRange(from, to))
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
        SearchResponse response = searchBuilder(indexNames)
                .setQuery(timeRange(from, to))
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

    private DateHistogramBuilder sumHistogramBuilder(String name, DateHistogramInterval interval, List<String> measurementIds) {
        DateHistogramBuilder builder = dateHistogram(name).field("timestamp").interval(interval);
        for (String measurementId : measurementIds)
            builder.subAggregation(sum(measurementId).field(measurementId));
        return builder;
    }

    private DateHistogramBuilder lastHistogramBuilder(String name, DateHistogramInterval interval, List<String> measurementIds) {
        DateHistogramBuilder builder = dateHistogram(name).field("timestamp").interval(interval);
        TopHitsBuilder topHitsBuilder = topHits(name).setSize(1).addSort("timestamp", SortOrder.DESC);
        measurementIds.forEach(topHitsBuilder::addField);
        return builder.subAggregation(topHitsBuilder);
    }

    private DateRangeBuilder dateRangeBuilder(String name, ZonedDateTime from, ZonedDateTime to, List<String> measurementIds) {
        DateRangeBuilder builder = dateRange(name).field(timeFieldName).addRange(formatTimestamp(from), formatTimestamp(to));
        for (String measurementId : measurementIds)
            builder.subAggregation(sum(measurementId).field(measurementId));
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
            long value = Long.valueOf(hit.getSource().get(field).toString());
            measurements.add(new Measurement(field, value));
        });
        return measurements;
    }

    private List<Measurement> measurements(Aggregations aggregations) {
        List<Measurement> measurements = new ArrayList<>();
        for (Aggregation agg : aggregations) {
            if(agg instanceof Sum) {
                measurements.add(new Measurement(agg.getName(), (long) ((Sum) agg).getValue()));
            }else if(agg instanceof InternalTopHits){
                Map<String, SearchHitField> fieldsMap = ((InternalTopHits) agg).getHits().getAt(0).fields();
                for (String s : fieldsMap.keySet()) {
                    Long value = (long) fieldsMap.get(s).getValues().get(0);
                    measurements.add(new Measurement(s, value));
                }
            }
        }
        return measurements;
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return dateTimeFormatter.format(timestamp);
    }

}
