package no.difi.statistics.query.elasticsearch.commands;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import java.util.List;

abstract class HistogramQuery extends Query {

    private static final String timeFieldName = "timestamp";

    public abstract List<TimeSeriesPoint> execute();

    DateHistogramAggregationBuilder dateHistogram(MeasurementDistance targetDistance) {
        return AggregationBuilders
                .dateHistogram(targetDistance.name())
                .field(timeFieldName)
                .dateHistogramInterval(dateHistogramInterval(targetDistance));
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
