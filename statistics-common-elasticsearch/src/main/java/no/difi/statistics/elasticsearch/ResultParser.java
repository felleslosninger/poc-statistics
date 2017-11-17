package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResultParser {

    private static final String timeFieldName = "timestamp";
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public static TimeSeriesPoint point(SearchHit hit) {
        return TimeSeriesPoint.builder().timestamp(timestamp(hit)).measurements(measurements(hit)).build();
    }

    public static TimeSeriesPoint pointFromLastAggregation(SearchResponse response) {
        return response.getAggregations() != null ?
            point(response.getAggregations().<TopHits>get("last").getHits().getAt(0)) : null;
    }

    public static TimeSeriesPoint point(Histogram.Bucket bucket) {
        return point(bucket.getKeyAsString(), bucket);
    }

    private static TimeSeriesPoint point(String timestamp, MultiBucketsAggregation.Bucket bucket) {
        return TimeSeriesPoint.builder().timestamp(timestamp(timestamp)).measurements(measurements(bucket.getAggregations())).build();
    }

    private static ZonedDateTime timestamp(SearchHit hit) {
        return timestamp(hit.getSource().get(timeFieldName).toString());
    }

    private static ZonedDateTime timestamp(String value) {
        return ZonedDateTime.parse(value, dateTimeFormatter);
    }

    private static List<Measurement> measurements(SearchHit hit) {
        List<Measurement> measurements = new ArrayList<>();
        hit.getSource().keySet().stream()
                .filter(field -> !field.equals(timeFieldName) && !field.startsWith("category."))
                .forEach(field -> {
                    long value = Long.valueOf(hit.getSource().get(field).toString());
                    measurements.add(new Measurement(field, value));
                });
        return measurements;
    }

    private static List<Measurement> measurements(Aggregations aggregations) {
        List<Measurement> measurements = new ArrayList<>();
        for (Aggregation aggregation : aggregations) {
            if (aggregation instanceof Sum) {
                measurements.add(new Measurement(aggregation.getName(), (long) ((Sum) aggregation).getValue()));
            } else if (aggregation instanceof TopHits) {
                long numHits = ((TopHits)aggregation).getHits().getHits().length;
                if (numHits != 1) throw new IllegalArgumentException("Expected 1 top hit but found " + numHits);
                Map<String, SearchHitField> fieldsMap = ((TopHits) aggregation).getHits().getAt(0).getFields();
                for (String s : fieldsMap.keySet()) {
                    Number value = (Number) fieldsMap.get(s).getValues().get(0);
                    measurements.add(new Measurement(s, value.longValue()));
                }
            }
        }
        return measurements;
    }

    public static TimeSeriesPoint sumPoint(Aggregations aggregations) {
        if (aggregations == null)
            return null;
        ZonedDateTime timestamp = timestamp(aggregations.<TopHits>get("last").getHits().getAt(0));
        return TimeSeriesPoint.builder().timestamp(timestamp).measurements(measurements(aggregations)).build();
    }

    public static TimeSeriesPoint sumPointFromRangeBucket(Range range) {
        if (range == null)
            return null;
        Range.Bucket bucket = range.getBuckets().get(0);
        return sumPoint(bucket.getAggregations());

    }

}
