package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ResultParser {

    private static final String timeFieldName = "timestamp";
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public static List<TimeSeriesPoint> points(MultiBucketsAggregation aggregation, Map<String, String> categories) {
        return aggregation.getBuckets().stream()
                .map(ResultParser::point)
                .map(p -> p.categories(categories).build())
                .collect(toList());
    }

    public static TimeSeriesPoint.Builder point(SearchHit hit) {
        return TimeSeriesPoint.builder()
                .timestamp(timestamp(hit))
                .measurements(measurements(hit))
                .categories(categories(hit));
    }

    public static TimeSeriesPoint pointFromLastAggregation(SearchResponse response, Map<String, String> categories) {
        if (response.getAggregations() == null)
            return null;
        if (response.getAggregations().get("last") == null)
            throw new RuntimeException("No last aggregation in result");
        if (response.getAggregations().<Terms>get("last").getBuckets().size() == 0)
            return null;
        if (response.getAggregations().<Terms>get("last").getBuckets().size() > 1)
            throw new RuntimeException("Too many buckets in last aggregation: "
                    + response.getAggregations().<Terms>get("last").getBuckets().size());
        return point(response.getAggregations().<Terms>get("last").getBuckets().get(0)).categories(categories).build();
    }

    private static TimeSeriesPoint.Builder point(Terms.Bucket bucket) {
        return TimeSeriesPoint.builder().timestamp(timestamp(bucket.getKeyAsString())).measurements(measurements(bucket.getAggregations()));
    }

    private static TimeSeriesPoint.Builder point(MultiBucketsAggregation.Bucket bucket) {
        return TimeSeriesPoint.builder().timestamp(timestamp(bucket.getKeyAsString())).measurements(measurements(bucket.getAggregations()));
    }

    private static ZonedDateTime timestamp(MultiBucketsAggregation.Bucket bucket) {
        return timestamp(bucket.getKeyAsString());
    }

    private static ZonedDateTime timestamp(SearchHit hit) {
        return timestamp(hit.getSourceAsMap().get(timeFieldName).toString());
    }

    private static ZonedDateTime timestamp(String value) {
        return ZonedDateTime.parse(value, dateTimeFormatter);
    }

    private static Map<String, Long> measurements(SearchHit hit) {
        return hit.getSourceAsMap().entrySet().stream()
                .filter(entry -> !entry.getKey().equals(timeFieldName) && !entry.getKey().startsWith("category."))
                .collect(toMap(Map.Entry::getKey, e -> Long.valueOf(e.getValue().toString())));
    }

    private static Map<String, String> categories(SearchHit hit) {
        return hit.getSourceAsMap().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("category."))
                .collect(toMap(entry -> entry.getKey().substring(9), entry -> entry.getValue().toString()));
    }

    private static Map<String, Long> measurements(Aggregations aggregations) {
        Map<String, Long> measurements = new HashMap<>();
        if (aggregations.get("timestampAggregation") != null) {
            Aggregation timestampAggregation = aggregations.get("timestampAggregation");
            aggregations = ((Terms)timestampAggregation).getBuckets().get(0).getAggregations();
        }
        for (Aggregation aggregation : aggregations) {
            if (aggregation instanceof Sum) {
                measurements.put(aggregation.getName(), (long) ((Sum) aggregation).getValue());
            } else if (aggregation instanceof TopHits) {
                long numHits = ((TopHits)aggregation).getHits().getHits().length;
                if (numHits != 1) throw new IllegalArgumentException("Expected 1 top hit but found " + numHits);
                Map<String, DocumentField> fieldsMap = ((TopHits) aggregation).getHits().getAt(0).getFields();
                for (String s : fieldsMap.keySet()) {
                    Number value = (Number) fieldsMap.get(s).getValues().get(0);
                    measurements.put(s, value.longValue());
                }
            }
        }
        return measurements;
    }

    public static TimeSeriesPoint.Builder sumPoint(Aggregations aggregations) {
        if (aggregations == null)
            return null;
        ZonedDateTime timestamp = timestamp(aggregations.<Terms>get("last").getBuckets().get(0));
        return TimeSeriesPoint.builder().timestamp(timestamp).measurements(measurements(aggregations));
    }

    public static TimeSeriesPoint.Builder sumPointFromRangeBucket(Range range) {
        if (range == null)
            return null;
        Range.Bucket bucket = range.getBuckets().get(0);
        Aggregations aggregations = bucket.getAggregations();
        ZonedDateTime timestamp = timestamp(aggregations.<TopHits>get("last").getHits().getAt(0));
        return TimeSeriesPoint.builder().timestamp(timestamp).measurements(measurements(aggregations));
    }

}
