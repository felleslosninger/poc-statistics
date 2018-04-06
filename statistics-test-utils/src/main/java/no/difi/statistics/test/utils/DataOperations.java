package no.difi.statistics.test.utils;

import com.tdunning.math.stats.TDigest;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.RelationalOperator;
import no.difi.statistics.model.TimeSeries;
import no.difi.statistics.model.TimeSeriesPoint;
import org.hamcrest.Matcher;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.time.temporal.ChronoUnit.*;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.elasticsearch.Timestamp.truncatedTimestamp;
import static no.difi.statistics.test.utils.TimeSeriesSumCollector.summarize;
import static org.hamcrest.Matchers.*;

public class DataOperations {

    private DataOperations() {
        throw new UnsupportedOperationException(getClass() + " does not support instantiation");
    }

    public static long sum(String measurementId, List<TimeSeriesPoint> points) {
        return points.stream().map(p -> p.getMeasurement(measurementId)).mapToLong(Optional::get).sum();
    }

    public static ZonedDateTime timestamp(int i, List<TimeSeriesPoint> timeSeries) {
        return timeSeries.get(i).getTimestamp();
    }

    public static long value(int index, String measurementId, List<TimeSeriesPoint> timeSeriesPoints){
        return timeSeriesPoints.get(index).getMeasurement(measurementId).orElseThrow(IllegalArgumentException::new);
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

    private static long measurementValue(String measurementId, TimeSeriesPoint point) {
        return point.getMeasurement(measurementId).orElseThrow(RuntimeException::new);
    }

    public static List<TimeSeriesPoint> sumPer(TimeSeries series, Map<String, String> categories, MeasurementDistance targetDistance) {
        List<TimeSeriesPoint> sums = new ArrayList<>(
                series.getPoints().stream()
                        // Discard points with irrelevant categories
                        .filter(point -> point.hasCategories(categories))
                        // Summarize per timestamp (as there might be several points with different categories per timestamp)
                        .collect(groupingBy(TimeSeriesPoint::getTimestamp, summarize()))
                        .values().stream()
                        // Then summarize per target distance
                        .collect(
                                groupingBy(
                                        point -> truncatedTimestamp(point.getTimestamp(), targetDistance),
                                        summarize(timestamp -> truncatedTimestamp(timestamp, targetDistance), categories)
                                )
                        )
                        .values());
        sums.sort(null);
        return sums;
    }

    public static List<TimeSeriesPoint> lastPer(TimeSeries series, Map<String, String> categories, MeasurementDistance targetDistance) {
        return series.getPoints().stream()
                // Discard points with irrelevant categories
                .filter(point -> point.hasCategories(categories))
                // Summarize per timestamp (as there might be several points with different categories per timestamp)
                .collect(groupingBy(TimeSeriesPoint::getTimestamp, summarize()))
                .values().stream().sorted() // Reestablish ordering after grouping
                // Group points by target distance
                .collect(groupingBy(point -> truncatedTimestamp(point.getTimestamp(), targetDistance)))
                .values().stream()
                // Pick last point bucket
                .map(list -> list.get(list.size() - 1))
                .map(point -> addCategories(point, categories))
                .sorted()
                .collect(toList());
    }

    private static TimeSeriesPoint addCategories(TimeSeriesPoint point, Map<String, String> categories) {
        return TimeSeriesPoint.builder().timestamp(point.getTimestamp()).measurements(point.getMeasurements()).categories(categories).build();
    }

    public static Function<TimeSeries, List<TimeSeriesPoint>> relativeToPercentile(
            RelationalOperator operator,
            String measurementId,
            int percentile
    ) {
        return series -> {
            Double collectedValue = percentileValue(percentile, measurementId).apply(series);
            return series.getPoints().stream()
                    .filter(point -> relationalMatcher(operator, collectedValue).matches(point.getMeasurement(measurementId).get().doubleValue()))
                    .collect(toList());
        };
    }

    private static Function<TimeSeries, Double> percentileValue(int percentile, String measurementId) {
        return (series) -> {
            TDigest tdigest = TDigest.createTreeDigest(100.0);
            series.getPoints().stream().map(p -> p.getMeasurement(measurementId).get()).forEach(tdigest::add);
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
