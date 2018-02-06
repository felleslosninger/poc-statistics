package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;

public class QueryBuilders {

    private static final String timestampField = "timestamp";
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public static RangeQueryBuilder timeRangeQuery(ZonedDateTime from, ZonedDateTime to) {
        RangeQueryBuilder builder = rangeQuery(timestampField);
        if (from != null)
            builder.from(dateTimeFormatter.format(from));
        if (to != null)
            builder.to(dateTimeFormatter.format(to));
        return builder;
    }

    public static MatchQueryBuilder categoryQuery(String key, String value) {
        return matchQuery("category." + key, value);
    }

    public static TermsAggregationBuilder sumPerTimestampAggregation(String name, List<String> measurementIds) {
        TermsAggregationBuilder builder = terms(name).field(timestampField).size(10_000);
        for (String measurementId : measurementIds)
            builder.subAggregation(sum(measurementId).field(measurementId));
        return builder;
    }

    public static DateHistogramAggregationBuilder sumPerDistanceAggregation(String name, MeasurementDistance targetDistance, List<String> measurementIds) {
        DateHistogramAggregationBuilder builder = dateHistogram(name).field(timestampField).dateHistogramInterval(dateHistogramInterval(targetDistance));
        for (String measurementId : measurementIds)
            builder.subAggregation(sum(measurementId).field(measurementId));
        return builder;
    }

    public static TopHitsAggregationBuilder lastAggregation() {
        return topHits("last").size(1).sort(timestampField, SortOrder.DESC);
    }

    public static DateHistogramAggregationBuilder lastPerDistanceAggregation(String name, MeasurementDistance targetDistance, List<String> measurementIds) {
        DateHistogramAggregationBuilder dateHistogramAggregation = dateHistogram(name).field(timestampField).dateHistogramInterval(dateHistogramInterval(targetDistance));
        TopHitsAggregationBuilder topHitsAggregation = topHits(name).size(1).sort(timestampField, SortOrder.DESC);
        measurementIds.forEach(topHitsAggregation::fieldDataField);
        return dateHistogramAggregation.subAggregation(topHitsAggregation);
    }

    public static DateRangeAggregationBuilder sumAggregation(String name, ZonedDateTime from, ZonedDateTime to, List<String> measurementIds) {
        DateRangeAggregationBuilder builder = dateRange(name).field(timestampField);
        if (from == null && to == null)
            throw new IllegalArgumentException("from or to required");
        else if (from == null)
            builder.addUnboundedTo(formatTimestamp(to));
        else if (to == null)
            builder.addUnboundedFrom(formatTimestamp(from));
        else
            builder.addRange(formatTimestamp(from), formatTimestamp(to));
        for (String measurementId : measurementIds)
            builder.subAggregation(sum(measurementId).field(measurementId));
        builder.subAggregation(lastAggregation()); // For timestamp on sum point
        return builder;
    }

    private static String formatTimestamp(ZonedDateTime timestamp) {
        return dateTimeFormatter.format(timestamp);
    }

    private static DateHistogramInterval dateHistogramInterval(MeasurementDistance distance) {
        switch (distance) {
            case minutes: return DateHistogramInterval.MINUTE;
            case hours: return DateHistogramInterval.HOUR;
            case days: return DateHistogramInterval.DAY;
            case months: return DateHistogramInterval.MONTH;
            case years: return DateHistogramInterval.YEAR;
            default: throw new IllegalArgumentException(distance.toString());
        }
    }

}
