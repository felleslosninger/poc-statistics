package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;

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

    public static DateHistogramAggregationBuilder sumHistogramAggregation(String name, MeasurementDistance targetDistance, List<String> measurementIds) {
        DateHistogramAggregationBuilder builder = dateHistogram(name).field(timestampField).dateHistogramInterval(dateHistogramInterval(targetDistance));
        for (String measurementId : measurementIds)
            builder.subAggregation(sum(measurementId).field(measurementId));
        return builder;
    }

    public static TopHitsAggregationBuilder lastAggregation() {
        return topHits("last").size(1).sort(timestampField, SortOrder.DESC);
    }

    public static DateHistogramAggregationBuilder lastHistogramAggregation(String name, MeasurementDistance targetDistance, List<String> measurementIds) {
        DateHistogramAggregationBuilder builder = dateHistogram(name).field(timestampField).dateHistogramInterval(dateHistogramInterval(targetDistance));
        TopHitsAggregationBuilder topHitsBuilder = topHits(name).size(1).sort(timestampField, SortOrder.DESC);
        measurementIds.forEach(topHitsBuilder::fieldDataField);
        return builder.subAggregation(topHitsBuilder);
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
