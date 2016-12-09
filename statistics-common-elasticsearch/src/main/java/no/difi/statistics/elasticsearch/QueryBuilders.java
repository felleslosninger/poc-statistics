package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;

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

    public static DateHistogramBuilder sumHistogramAggregation(String name, MeasurementDistance targetDistance, List<String> measurementIds) {
        DateHistogramBuilder builder = dateHistogram(name).field(timestampField).interval(dateHistogramInterval(targetDistance));
        for (String measurementId : measurementIds)
            builder.subAggregation(sum(measurementId).field(measurementId));
        return builder;
    }

    public static TopHitsBuilder lastAggregation() {
        return topHits("last").setSize(1).addSort(timestampField, SortOrder.DESC);
    }

    public static DateHistogramBuilder lastHistogramAggregation(String name, MeasurementDistance targetDistance, List<String> measurementIds) {
        DateHistogramBuilder builder = dateHistogram(name).field(timestampField).interval(dateHistogramInterval(targetDistance));
        TopHitsBuilder topHitsBuilder = topHits(name).setSize(1).addSort(timestampField, SortOrder.DESC);
        measurementIds.forEach(topHitsBuilder::addField);
        return builder.subAggregation(topHitsBuilder);
    }

    public static DateRangeBuilder sumAggregation(String name, ZonedDateTime from, ZonedDateTime to, List<String> measurementIds) {
        DateRangeBuilder builder = dateRange(name).field(timestampField);
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
