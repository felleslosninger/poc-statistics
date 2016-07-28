package no.difi.statistics.test.utils;

import com.tdunning.math.stats.TDigest;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class DataOperations {

    private DataOperations() {
        throw new UnsupportedOperationException(getClass() + " does not support instantiation");
    }

    public static int sum(String measurementId, List<TimeSeriesPoint> points) {
        return points.stream().map(p -> p.getMeasurement(measurementId)).map(Optional::get).mapToInt(Measurement::getValue).sum();
    }

    public static ZonedDateTime timestamp(int i, List<TimeSeriesPoint> timeSeries) throws IOException {
        return timeSeries.get(i).getTimestamp();
    }

    public static ZonedDateTime truncate(ZonedDateTime timestamp, ChronoUnit toUnit) {
        switch (toUnit) {
            case YEARS:
                return ZonedDateTime.of(timestamp.getYear(), 1, 1, 0, 0, 0, 0, timestamp.getZone());
            case MONTHS:
                return ZonedDateTime.of(timestamp.getYear(), timestamp.getMonthValue(), 1, 0, 0, 0, 0, timestamp.getZone());
            case DAYS:
                return ZonedDateTime.of(timestamp.getYear(), timestamp.getMonthValue(), timestamp.getDayOfMonth(), 0, 0, 0, 0, timestamp.getZone());
        }
        return timestamp.truncatedTo(toUnit);
    }

    public static int percentileIndex(int percent, int dataSize) {
        int index = new BigDecimal(percent).multiply(new BigDecimal(dataSize))
                .divide(new BigDecimal(100), 0, RoundingMode.HALF_UP).intValue();
        return index - 1; // Index is zero based
    }

    public static int[] sort(int[] src) {
        int[] dst = src.clone();
        Arrays.sort(dst);
        return dst;
    }

    public static int size(List<TimeSeriesPoint> timeSeries) {
        return timeSeries.size();
    }

    public static int measurementValue(String measurementId, int i, List<TimeSeriesPoint> timeSeries) {
        return measurementValue(measurementId, timeSeries.get(i));
    }

    public static int measurementValue(String measurementId, TimeSeriesPoint point) {
        return point.getMeasurement(measurementId).map(Measurement::getValue).orElseThrow(RuntimeException::new);
    }

    public static void assertPercentile(int percent, int[] points, String measurementId, List<TimeSeriesPoint> resultingPoints) {
        int percentileIndex = percentileIndex(percent, points.length);
        int expectedPercentileValue = sort(points)[percentileIndex];
        assertEquals(points.length - (percentileIndex + 1), size(resultingPoints));
        resultingPoints.forEach(point -> assertThat(measurementValue(measurementId, point), greaterThanOrEqualTo(expectedPercentileValue)));
    }

    public static void assertPercentileTDigest(int percent, int[] points, String measurementId, List<TimeSeriesPoint> resultingPoints) {
        TDigest tdigest = TDigest.createTreeDigest(100.0);
        for (int point : points)
            tdigest.add(point);
        double expectedPercentileValue = tdigest.quantile(new BigDecimal(percent).divide(new BigDecimal(100), 2, RoundingMode.HALF_UP).doubleValue());
        resultingPoints.forEach(point ->
                assertThat(Integer.valueOf(measurementValue(measurementId, point)).doubleValue(), greaterThanOrEqualTo(expectedPercentileValue))
        );
    }

}
