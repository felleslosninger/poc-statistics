package no.difi.statistics.test.utils;

import com.tdunning.math.stats.TDigest;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.RelationalOperator;
import no.difi.statistics.model.TimeSeriesPoint;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;

import static java.time.temporal.ChronoUnit.*;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.Timestamp.truncate;
import static no.difi.statistics.test.utils.TimeSeriesSumCollector.summarize;
import static org.hamcrest.Matchers.*;

public class DataOperations {

    private DataOperations() {
        throw new UnsupportedOperationException(getClass() + " does not support instantiation");
    }

    public static long sum(String measurementId, List<TimeSeriesPoint> points) {
        return points.stream().map(p -> p.getMeasurement(measurementId)).map(Optional::get).mapToLong(Measurement::getValue).sum();
    }

    public static ZonedDateTime timestamp(int i, List<TimeSeriesPoint> timeSeries) throws IOException {
        return timeSeries.get(i).getTimestamp();
    }

    public static long value(int index, String measurementId, List<TimeSeriesPoint> timeSeriesPoints){
        return timeSeriesPoints.get(index).getMeasurement(measurementId).map(Measurement::getValue).orElseThrow(IllegalArgumentException::new);
    }

    public static ChronoUnit unit(MeasurementDistance distance) {
        switch (distance) {
            case minutes: return MINUTES;
            case hours: return HOURS;
            case days: return DAYS;
            case months: return MONTHS;
            case years: return YEARS;
            default: throw new IllegalArgumentException(distance.toString());
        }
    }

    public static int size(List<TimeSeriesPoint> timeSeries) {
        return timeSeries.size();
    }

    public static long measurementValue(String measurementId, int i, List<TimeSeriesPoint> timeSeries) {
        return measurementValue(measurementId, timeSeries.get(i));
    }

    public static long measurementValue(String measurementId, TimeSeriesPoint point) {
        return point.getMeasurement(measurementId).map(Measurement::getValue).orElseThrow(RuntimeException::new);
    }

    public static List<TimeSeriesPoint> sumPer(List<TimeSeriesPoint> points, MeasurementDistance distance) {
        List<TimeSeriesPoint> sums = new ArrayList<>(
                points.stream()
                        .collect(groupingBy(point -> truncate(point.getTimestamp(), distance),
                                summarize(timestamp -> truncate(timestamp, distance)))
                        )
                .values());
        sums.sort(null);
        return sums;
    }

    public static List<TimeSeriesPoint> lastPer(List<TimeSeriesPoint> points, MeasurementDistance distance) {
        Map<ZonedDateTime,TimeSeriesPoint> unitMap = new HashMap<>();
        points.forEach(point -> unitMap.put(truncate(point.getTimestamp(), distance), normalizeTimestamp(point, distance)));
        List<TimeSeriesPoint> result = new ArrayList<>(unitMap.values());
        result.sort(null);
        return result;
    }

    private static TimeSeriesPoint normalizeTimestamp(TimeSeriesPoint point, MeasurementDistance distance) {
        return TimeSeriesPoint.builder().timestamp(truncate(point.getTimestamp(), distance)).measurements(point.getMeasurements()).build();
    }

    public static Function<List<TimeSeriesPoint>, List<TimeSeriesPoint>> relativeToPercentile(RelationalOperator operator, String measurementId, int percentile) {
        return points -> {
            Double collectedValue = percentileValue(percentile, measurementId).apply(points);
            return points.stream()
                    .filter(point -> relationalMatcher(operator, collectedValue).matches(Long.valueOf(point.getMeasurement(measurementId).get().getValue()).doubleValue()))
                    .collect(toList());
        };
    }

    private static Function<List<TimeSeriesPoint>, Double> percentileValue(int percentile, String measurementId) {
        return (points) -> {
            TDigest tdigest = TDigest.createTreeDigest(100.0);
            points.forEach(point -> tdigest.add(point.getMeasurement(measurementId).get().getValue()));
            return tdigest.quantile(new BigDecimal(percentile).divide(new BigDecimal(100), 2, RoundingMode.HALF_UP).doubleValue());
        };
    }

    private static <T extends Comparable<T>> Matcher<T> relationalMatcher(RelationalOperator operator, T value) {
        switch (operator) {
            case gt: return greaterThan(value);
            case gte: return greaterThanOrEqualTo(value);
            case lt: return lessThan(value);
            case lte: return lessThanOrEqualTo(value);
            default: throw new IllegalArgumentException(operator.toString());
        }
    }

}
