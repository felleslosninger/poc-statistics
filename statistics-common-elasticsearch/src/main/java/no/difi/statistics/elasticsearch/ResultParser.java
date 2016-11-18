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
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;
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
        return TimeSeriesPoint.builder().timestamp(time(hit)).measurements(measurements(hit)).build();
    }

    public static TimeSeriesPoint pointFromLastAggregation(SearchResponse response) {
        return point(response.getAggregations().<TopHits>get("last").getHits().getAt(0));
    }

    public static TimeSeriesPoint point(Histogram.Bucket bucket) {
        return point(bucket.getKeyAsString(), bucket);
    }

    public static TimeSeriesPoint point(Range.Bucket bucket) {
        return point(bucket.getFromAsString(), bucket);
    }

    private static TimeSeriesPoint point(String timestamp, MultiBucketsAggregation.Bucket bucket) {
        return TimeSeriesPoint.builder().timestamp(time(timestamp)).measurements(measurements(bucket.getAggregations())).build();
    }

    private static ZonedDateTime time(SearchHit hit) {
        return time(hit.getSource().get(timeFieldName).toString());
    }

    private static ZonedDateTime time(String value) {
        return ZonedDateTime.parse(value, dateTimeFormatter);
    }

    private static List<Measurement> measurements(SearchHit hit) {
        List<Measurement> measurements = new ArrayList<>();
        hit.getSource().keySet().stream().filter(field -> !field.equals(timeFieldName)).forEach(field -> {
            long value = Long.valueOf(hit.getSource().get(field).toString());
            measurements.add(new Measurement(field, value));
        });
        return measurements;
    }

    private static List<Measurement> measurements(Aggregations aggregations) {
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

}
