package no.difi.statistics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.restassured.response.Response;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.test.utils.DataGenerator;
import no.difi.statistics.test.utils.DockerHelper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static no.difi.statistics.test.utils.DataOperations.*;
import static org.junit.Assert.assertEquals;

public abstract class AbstractQueryIT {

    private final static String timeSeriesName = "test";
    private final static String measurementId = "count";
    private ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 13, 30, 31, 123, ZoneId.of("UTC"));
    private String apiBaseUrl;
    private static ObjectMapper objectMapper;
    final static DockerHelper dockerHelper = new DockerHelper();

    @BeforeClass
    public static void initAbstractAll() throws UnknownHostException {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Before
    public void init() {
        apiBaseUrl = format(
                "http://%s:%s",
                dockerHelper.address(),
                dockerHelper.portFor(8080, apiContainerName())
        );
    }

    protected abstract String apiContainerName();

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

    @Test
    public void givenHourSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexHourPointsFrom(now.minusHours(1003), 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = hours(timeSeriesName, now.minusHours(1002), now.minusHours(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexHourPointsFrom(now.minusHours(20), 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = hours(timeSeriesName, now.minusHours(9), now.minusHours(8));
        assertEquals(0, size(timeSeries));
    }

    @Test @Ignore
    public void givenHourSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexHourPoint(now.minusHours(100), 100);
        indexHourPoint(now.minusHours(200), 200);
        indexHourPoint(now.minusHours(300), 300);
        indexHourPoint(now.minusHours(400), 400);
        List<TimeSeriesPoint> timeSeries = hours(timeSeriesName, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexDayPointsFrom(now.minusDays(1003), 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = days(timeSeriesName, now.minusDays(1002), now.minusDays(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexDayPointsFrom(now.minusDays(20), 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = days(timeSeriesName, now.minusDays(9), now.minusDays(8));
        assertEquals(0, size(timeSeries));
    }

    @Test @Ignore
    public void givenDaySeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexDayPoint(now.minusDays(100), 100);
        indexDayPoint(now.minusDays(200), 200);
        indexDayPoint(now.minusDays(300), 300);
        indexDayPoint(now.minusDays(400), 400);
        List<TimeSeriesPoint> timeSeries = days(timeSeriesName, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexMonthPointsFrom(now.minusMonths(1003), 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = months(timeSeriesName, now.minusMonths(1002), now.minusMonths(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexMonthPointsFrom(now.minusMonths(20), 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = months(timeSeriesName, now.minusMonths(9), now.minusMonths(8));
        assertEquals(0, size(timeSeries));
    }

    @Test @Ignore
    public void givenMonthSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexMonthPoint(now.minusMonths(100), 100);
        indexMonthPoint(now.minusMonths(200), 200);
        indexMonthPoint(now.minusMonths(300), 300);
        indexMonthPoint(now.minusMonths(400), 400);
        List<TimeSeriesPoint> timeSeries = months(timeSeriesName, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexYearPointsFrom(now.minusYears(4), 4, 3, 2, 1);
        List<TimeSeriesPoint> timeSeries = years(timeSeriesName, now.minusYears(3), now.minusYears(2));
        assertEquals(2, size(timeSeries));
        assertEquals(3, measurementValue(measurementId, 0, timeSeries));
        assertEquals(2, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexYearPointsFrom(now.minusYears(20), 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = years(timeSeriesName, now.minusYears(9), now.minusYears(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexYearPointsFrom(now.minusYears(4), 4, 3, 2, 1);
        List<TimeSeriesPoint> timeSeries = years(timeSeriesName, now.minusYears(10), now.plusYears(10));
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

    @Test
    public void givenMinuteSeriesWhenQueryingForMonthPointsThenSummarizedMinutesAreReturned() throws IOException {
        List<TimeSeriesPoint> points = DataGenerator.createRandomTimeSeries(now.truncatedTo(DAYS), ChronoUnit.MINUTES, 100, "measurementA", "measurementB");
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

    private List<TimeSeriesPoint> parseJson(String json) throws IOException {
        return objectMapper.readValue(json, new TypeReference<List<TimeSeriesPoint>>(){});
    }

    protected abstract void indexMinutePoints(List<TimeSeriesPoint> minutePoints) throws IOException;

    protected abstract void indexMinutePointsFrom(ZonedDateTime timestamp, int...values) throws IOException;

    protected abstract void indexHourPointsFrom(ZonedDateTime timestamp, int...values) throws IOException;

    protected abstract void indexDayPointsFrom(ZonedDateTime timestamp, int...values) throws IOException;

    protected abstract void indexMonthPointsFrom(ZonedDateTime timestamp, int...values) throws IOException;

    protected abstract void indexYearPointsFrom(ZonedDateTime timestamp, int...values) throws IOException;

    protected abstract void indexMinutePoint(ZonedDateTime timestamp, int value) throws IOException;

    protected abstract void indexHourPoint(ZonedDateTime timestamp, int value) throws IOException;

    protected abstract void indexDayPoint(ZonedDateTime timestamp, int value) throws IOException;

    protected abstract void indexMonthPoint(ZonedDateTime timestamp, int value) throws IOException;

    private List<TimeSeriesPoint> minutes(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return parseJson(get(urlForSeries("minutes", seriesName, from, to)).asString());
    }

    private List<TimeSeriesPoint> minutesAbovePercentile(int percentile, String measurementId, String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        Response response = given()
                .contentType("application/json")
                .body(format("{\"percentile\":%d,\"measurementId\":\"%s\"}", percentile, measurementId)).when()
                .post(urlForSeries("minutes", seriesName, from, to));
        return parseJson(response.getBody().asString());
    }

    private List<TimeSeriesPoint> hours(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return parseJson(get(urlForSeries("hours", seriesName, from, to)).asString());
    }

    private List<TimeSeriesPoint> days(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return parseJson(get(urlForSeries("days", seriesName, from, to)).asString());
    }

    private List<TimeSeriesPoint> months(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return parseJson(get(urlForSeries("months", seriesName, from, to)).asString());
    }

    private List<TimeSeriesPoint> years(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return parseJson(get(urlForSeries("years", seriesName, from, to)).asString());
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

    private String urlForSeries(String seriesTimeUnit, String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return format(
                "%s/%s/%s/%s?from=%s&to=%s",
                apiBaseUrl,
                seriesTimeUnit,
                seriesName,
                "total",
                formatTimestamp(from),
                formatTimestamp(to)
        );
    }

}
