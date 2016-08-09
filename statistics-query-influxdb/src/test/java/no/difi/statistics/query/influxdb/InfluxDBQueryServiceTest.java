package no.difi.statistics.query.influxdb;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.config.AppConfig;
import no.difi.statistics.query.influxdb.config.InfluxDBConfig;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoUnit.DAYS;
import static no.difi.statistics.test.utils.DataGenerator.createRandomTimeSeries;
import static no.difi.statistics.test.utils.DataOperations.*;
import static org.junit.Assert.assertEquals;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;

@SpringBootTest(
        webEnvironment = RANDOM_PORT
)
@ContextConfiguration(classes = {AppConfig.class, InfluxDBConfig.class}, initializers = InfluxDBQueryServiceTest.Initializer.class)
@RunWith(SpringRunner.class)
public class InfluxDBQueryServiceTest {

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            GenericContainer influxDB = new GenericContainer("influxdb:0.13.0");
            influxDB.start();
            EnvironmentTestUtils.addEnvironment(
                    applicationContext.getEnvironment(),
                    "no.difi.statistics.influxdb.host=" + influxDB.getContainerIpAddress(),
                    "no.difi.statistics.influxdb.port=" + influxDB.getMappedPort(8086)
            );
            InfluxDBQueryServiceTest.backend = influxDB;
        }

    }

    private ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 13, 30, 31, 123, ZoneId.of("UTC"));
    private final static String measurementId = "count";
    private static final String databaseName = "default";
    private final static String timeSeriesName = "test";

    private static GenericContainer backend;

    @Autowired
    private InfluxDB client;
    @Autowired
    private TestRestTemplate restTemplate;

    @After
    public void after() {
        client.deleteDatabase(databaseName);
    }

    @AfterClass
    public static void tearDown() {
        backend.stop();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexMinutePointsFrom(now.minusMinutes(1003), 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = minutes(timeSeriesName, now.minusMinutes(1002), now.minusMinutes(1001));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexMinutePointsFrom(now.minusMinutes(20), 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = minutes(timeSeriesName, now.minusMinutes(9), now.minusMinutes(8));
        assertEquals(0, size(timeSeries));
    }

    @Test @Ignore
    public void givenMinuteSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexMinutePoint(now.minusMinutes(100), 100);
        indexMinutePoint(now.minusMinutes(200), 200);
        indexMinutePoint(now.minusMinutes(300), 300);
        indexMinutePoint(now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = minutes(timeSeriesName, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test // 1, 2, 3, 4, 5, 8, 21, 44, 55, 89, 131, 200, 700, 1000
    public void givenMinuteSeriesWithSize14WhenQueryingForDataPointsWithMeasurementAbove92ndPercentileThenLargestMeasurementsFromPosition13AreReturned() throws IOException {
        int[] points = {1000, 4, 700, 1, 2, 3, 5, 44, 8, 21, 200, 131, 55, 89};
        indexMinutePointsFrom(now.minusMinutes(50), points);
        List<TimeSeriesPoint> resultingPoints = minutesAbovePercentile(
                92, measurementId, timeSeriesName,
                now.minusMinutes(100), now.minusMinutes(0)
        );
        assertPercentile(92, points, measurementId, resultingPoints);
    }

    @Test // 3, 5, 11, 13, 56, 234, 235, 546, 566, 574, 674, 777, 1244, 3454, 3455, 5667, 9000, 547547
    public void givenMinuteSeriesWithSize18WhenQueryingForDataPointsWithMeasurementAbove35thPercentileThenLargestMeasurementsFromPosition6AreReturned() throws IOException {
        int[] points = {13, 11, 546, 234, 3455, 547547, 574, 3, 3454, 5, 1244, 674, 566, 5667, 56, 777, 235, 9000};
        indexMinutePointsFrom(now.minusMinutes(300), points);
        List<TimeSeriesPoint> resultingPoints = minutesAbovePercentile(
                40, measurementId, timeSeriesName,
                now.minusMinutes(301), now.minusMinutes(0)
        );
        assertPercentile(40, points, measurementId, resultingPoints);
    }

    @Test @Ignore("InfluxDB does not support sum aggregation per month")
    public void givenMinuteSeriesWhenQueryingForMonthPointsThenSummarizedMinutesAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), ChronoUnit.MINUTES, 100, "measurementA", "measurementB");
        indexMinutePoints(points);
        List<TimeSeriesPoint> resultingPoints = months(
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(100)
        );
        assertEquals(1, size(resultingPoints));
        assertEquals(sum("measurementA", points), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1).intValue());
        assertEquals(sum("measurementB", points), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1).intValue());
        assertEquals(truncate(now, ChronoUnit.MONTHS).toInstant(), timestamp(0, resultingPoints).toInstant());
    }

    private List<TimeSeriesPoint> minutes(String timeSeriesName, ZonedDateTime from, ZonedDateTime to) {
        return getRequest("/minutes/{seriesName}?from={from}&to={to}", timeSeriesName, from, to);
    }

    private List<TimeSeriesPoint> months(String timeSeriesName, ZonedDateTime from, ZonedDateTime to) {
        return getRequest("/months/{seriesName}?from={from}&to={to}", timeSeriesName, from, to);
    }

    private List<TimeSeriesPoint> minutesAbovePercentile(int percentile, String measurementId, String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return restTemplate.exchange(
                "/minutes/{seriesName}?from={from}&to={to}",
                POST,
                new HttpEntity<>(new TimeSeriesFilter(percentile, measurementId)),
                new ParameterizedTypeReference<List<TimeSeriesPoint>>(){},
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        ).getBody();
    }

    private List<TimeSeriesPoint> getRequest(
            String url,
            String timeSeriesName,
            ZonedDateTime from,
            ZonedDateTime to
    ) {
        return restTemplate.exchange(
                url,
                GET,
                null,
                new ParameterizedTypeReference<List<TimeSeriesPoint>>(){},
                timeSeriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        ).getBody();
    }

    private void indexMinutePoint(ZonedDateTime timestamp, int value) {
        ingestTimeSeriesPoint(timeSeriesName, TimeSeriesPoint.builder().timestamp(timestamp).measurement(measurementId, value).build());
    }

    private void indexMinutePoints(List<TimeSeriesPoint> points) {
        points.forEach(point -> ingestTimeSeriesPoint(timeSeriesName, point));
    }

    private void indexMinutePointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            ingestTimeSeriesPoint(timeSeriesName, TimeSeriesPoint.builder().timestamp(timestamp).measurement(measurementId, value).build());
            timestamp = timestamp.plusMinutes(1);
        }
    }

    private void ingestTimeSeriesPoint(String timeSeriesName, TimeSeriesPoint dataPoint) {
        client.createDatabase(databaseName); // Does a CREATE DATABASE IF NOT EXISTS
        Point.Builder influxPoint = Point.measurement(timeSeriesName)
                .time(dataPoint.getTimestamp().toInstant().toEpochMilli(), TimeUnit.MILLISECONDS);
        for (Measurement measurement : dataPoint.getMeasurements())
            influxPoint.addField(measurement.getId(), measurement.getValue());
        client.write(databaseName, null, influxPoint.build());
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

}
