package no.difi.statistics.test.utils;

import no.difi.statistics.model.TimeSeriesPoint;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerator {

    private DataGenerator() {
        throw new UnsupportedOperationException(getClass() + " does not support instantiation");
    }

    public static List<TimeSeriesPoint> createRandomTimeSeries(ZonedDateTime from, ChronoUnit unit, long size, String...measurementIds) {
        List<TimeSeriesPoint> points = new ArrayList<>();
        Random randomGenerator = new Random();
        ZonedDateTime timestamp = from;
        for (int i = 0; i < size; i++) {
            TimeSeriesPoint.Builder pointBuilder = TimeSeriesPoint.builder().timestamp(timestamp);
            for (int j = 0; j < measurementIds.length; j++) {
                pointBuilder.measurement(measurementIds[j], randomGenerator.nextInt(1_000));
            }
            points.add(pointBuilder.build());
            timestamp = timestamp.plus(1, unit);
        }
        return points;
    }


}
