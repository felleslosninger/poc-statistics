package no.difi.statistics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.config.AppConfig;
import no.difi.statistics.query.elasticsearch.config.ElasticsearchConfig;
import no.difi.statistics.test.utils.DataOperations;
import no.difi.statistics.test.utils.ElasticsearchHelper;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.time.temporal.ChronoUnit.*;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.model.MeasurementDistance.*;
import static no.difi.statistics.test.utils.DataGenerator.createRandomTimeSeries;
import static no.difi.statistics.test.utils.DataOperations.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = {AppConfig.class, ElasticsearchConfig.class}, initializers = ElasticsearchQueryServiceTest.Initializer.class)
@RunWith(SpringRunner.class)
public class ElasticsearchQueryServiceTest {

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            GenericContainer backend = new GenericContainer("elasticsearch:2.4.1");
            backend.start();
            EnvironmentTestUtils.addEnvironment(
                    applicationContext.getEnvironment(),
                    "no.difi.statistics.elasticsearch.host=" + backend.getContainerIpAddress(),
                    "no.difi.statistics.elasticsearch.port=" + backend.getMappedPort(9300)
            );
            ElasticsearchQueryServiceTest.backend = backend;
        }

    }

    private final static ZoneId UTC = ZoneId.of("UTC");
    private ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 0, 0, 0, 0, UTC);
    private final static String measurementId = "count";
    private final static String timeSeriesName = "test";
    private final static String anotherTimeSeriesName = "anothertimeseriesname";
    private final static String owner = "test_owner"; // Index names must be lower case in Elasticsearch

    private static GenericContainer backend;

    @Autowired
    private Client client;
    @Autowired
    private TestRestTemplate restTemplate;
    private ElasticsearchHelper elasticsearchHelper;
    @Autowired
    private ObjectMapper objectMapper;

    @Before
    public void prepare() throws Exception {
        elasticsearchHelper = new ElasticsearchHelper(
                client,
                backend.getContainerIpAddress(),
                backend.getMappedPort(9200)
        );
        elasticsearchHelper.waitForGreenStatus();
    }

    @After
    public void cleanup() {
        elasticsearchHelper.clear();
    }

    @AfterClass
    public static void cleanupAll() {
        backend.stop();
    }

    private void indexMinutePoints() throws IOException{
        indexPoint(timeSeriesName, minutes, now.minusMinutes(2), 1002);
        indexPoint(timeSeriesName, minutes, now, 1003);
        indexPoint(anotherTimeSeriesName, minutes, now, 42);
    }

    @Test
    public void givenTimeSeriesWhenQueryingForAvailableTimeSeriesThenAvailableTimeSeriesAreReturned() throws IOException, ExecutionException, InterruptedException {
        indexMinutePoints();

        List<String> availableTimeSeries = availableTimeSeries(owner);

        assertEquals(2, availableTimeSeries.size());
        assertTrue(availableTimeSeries.contains(timeSeriesName));
        assertTrue(availableTimeSeries.contains(anotherTimeSeriesName));
    }

    @Test
    public void givenTimeSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws IOException, ExecutionException, InterruptedException {
        indexPointsFrom(dateTime(2007, 1, 1, 11, 11), minutes, 1, 2, 3, 4, 5, 6, 7, 8, 9); // Some random "old" points
        TimeSeriesPoint expectedLastPoint = indexPoint(minutes, dateTime(2016, 3, 3, 12, 12), 123L);
        indexPoint(minutes, dateTime(2016, 3, 4, 1, 2), 5675L);
        TimeSeriesPoint actualLastPoint = last(timeSeriesName, owner, dateTime(2007, 1, 1, 0, 0), dateTime(2016, 3, 3, 13, 0));
        assertEquals(expectedLastPoint, actualLastPoint);
    }

    private ZonedDateTime dateTime(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, UTC);
    }

    @Test
    public void givenTimeSeriesWhenQueryingForAvailableTimeSeriesWithAnotherOwnerThenNoTimeSeriesAreReturned() throws IOException, ExecutionException, InterruptedException {
        indexMinutePoints();

        List<String> availableTimeSeries = availableTimeSeries("anotherOwner");

        assertEquals(0, availableTimeSeries.size());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusMinutes(1003), minutes, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(minutes, timeSeriesName, now.minusMinutes(1002), now.minusMinutes(1001));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMinuteSeriesLastingTwoDaysWhenQueryingForRangeOverThoseDaysThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexPoint(minutes, now.minusDays(1), 13);
        indexPoint(minutes, now, 117);
        List<TimeSeriesPoint> timeSeries = request(minutes, timeSeriesName, now.minusDays(1).minusHours(1), now);
        assertEquals(13, measurementValue(measurementId, 0, timeSeries));
        assertEquals(117, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusMinutes(20), minutes, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(minutes, timeSeriesName, now.minusMinutes(9), now.minusMinutes(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexPoint(minutes, now.minusMinutes(100), 100);
        indexPoint(minutes, now.minusMinutes(200), 200);
        indexPoint(minutes, now.minusMinutes(300), 300);
        indexPoint(minutes, now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = request(minutes, timeSeriesName, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithoutRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexPoint(minutes, now.minusMinutes(100), 100);
        indexPoint(minutes, now.minusMinutes(200), 200);
        indexPoint(minutes, now.minusMinutes(300), 300);
        indexPoint(minutes, now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = request(minutes, timeSeriesName);
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithLeftOpenRangeThenCorrectDataPointsAreReturned() throws IOException, InterruptedException {
        indexPoint(minutes, now.minusMinutes(100), 100);
        indexPoint(minutes, now.minusMinutes(200), 200);
        indexPoint(minutes, now.minusMinutes(300), 300);
        indexPoint(minutes, now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = requestTo(minutes, timeSeriesName, now.minusMinutes(101));
        assertEquals(3, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithRightOpenRangeThenCorrectDataPointsAreReturned() throws IOException, InterruptedException {
        indexPoint(minutes, now.minusMinutes(100), 100);
        indexPoint(minutes, now.minusMinutes(200), 200);
        indexPoint(minutes, now.minusMinutes(300), 300);
        indexPoint(minutes, now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = requestFrom(minutes, timeSeriesName, now.minusMinutes(101));
        assertEquals(1, size(timeSeries));
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusHours(1003), hours, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(hours, timeSeriesName, now.minusHours(1002), now.minusHours(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusHours(20), hours, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(hours, timeSeriesName, now.minusHours(9), now.minusHours(8));
        assertEquals(0, size(timeSeries));
    }

    @Test @Ignore
    public void givenHourSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexPoint(hours, now.minusHours(100), 100);
        indexPoint(hours, now.minusHours(200), 200);
        indexPoint(hours, now.minusHours(300), 300);
        indexPoint(hours, now.minusHours(400), 400);
        List<TimeSeriesPoint> timeSeries = request(hours, timeSeriesName, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusDays(1003), days, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(days, timeSeriesName, now.minusDays(1002), now.minusDays(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusDays(20), days, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(days, timeSeriesName, now.minusDays(9), now.minusDays(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexPoint(days, now.minusDays(100), 100);
        indexPoint(days, now.minusDays(200), 200);
        indexPoint(days, now.minusDays(300), 300);
        indexPoint(days, now.minusDays(400), 400);
        List<TimeSeriesPoint> timeSeries = request(days, timeSeriesName, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusMonths(1003), months, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(months, timeSeriesName, now.minusMonths(1002), now.minusMonths(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusMonths(20), months, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(months, timeSeriesName, now.minusMonths(9), now.minusMonths(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexPoint(months, now.minusMonths(10), 100);
        indexPoint(months, now.minusMonths(20), 200);
        indexPoint(months, now.minusMonths(30), 300);
        indexPoint(months, now.minusMonths(40), 400);
        List<TimeSeriesPoint> timeSeries = request(months, timeSeriesName, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusYears(4), years, 4, 3, 2, 1);
        List<TimeSeriesPoint> timeSeries = request(years, timeSeriesName, now.minusYears(3), now.minusYears(2));
        assertEquals(2, size(timeSeries));
        assertEquals(3, measurementValue(measurementId, 0, timeSeries));
        assertEquals(2, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusYears(20), years, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(years, timeSeriesName, now.minusYears(9), now.minusYears(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexPointsFrom(now.minusYears(4), years, 4, 3, 2, 1);
        List<TimeSeriesPoint> timeSeries = request(years, timeSeriesName, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test // 1, 2, 3, 4, 5, 8, 21, 44, 55, 89, 131, 200, 700, 1000
    public void givenMinuteSeriesWithSize14WhenQueryingForDataPointsWithMeasurementAbove92ndPercentileThenLargestMeasurementsFromPosition13AreReturned() throws IOException {
        long[] points = {1000, 4, 700, 1, 2, 3, 5, 44, 8, 21, 200, 131, 55, 89};
        indexPointsFrom(now.minusMinutes(50), minutes, points);
        List<TimeSeriesPoint> resultingPoints = minutesAbovePercentile(
                92, measurementId, timeSeriesName,
                now.minusMinutes(100), now.minusMinutes(0)
        );
        assertPercentileTDigest(92, points, measurementId, resultingPoints);
    }

    @Test // 3, 5, 11, 13, 56, 234, 235, 546, 566, 574, 674, 777, 1244, 3454, 3455, 5667, 9000, 547547
    public void givenMinuteSeriesWithSize18WhenQueryingForDataPointsWithMeasurementAbove35thPercentileThenLargestMeasurementsFromPosition6AreReturned() throws IOException {
        long[] points = {13, 11, 546, 234, 3455, 547547, 574, 3, 3454, 5, 1244, 674, 566, 5667, 56, 777, 235, 9000};
        indexPointsFrom(now.minusMinutes(300), minutes, points);
        List<TimeSeriesPoint> resultingPoints = minutesAbovePercentile(
                40, measurementId, timeSeriesName,
                now.minusMinutes(301), now.minusMinutes(0)
        );
        assertPercentileTDigest(40, points, measurementId, resultingPoints);
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForMonthPointsThenSummarizedMinutesAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), minutes, 100, "measurementA", "measurementB");
        indexPoints(minutes, points);
        List<TimeSeriesPoint> resultingPoints = request(
                months,
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(100)
        );
        assertEquals(1, size(resultingPoints));
        assertEquals(DataOperations.sum("measurementA", points), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementB", points), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(truncate(now, MONTHS).toInstant(), timestamp(0, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForMonthSnapshotsThenLastPointInMonthAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), minutes, 100, "measurementA", "measurementB");
        indexPoints(minutes, points);
        List<TimeSeriesPoint> resultingPoints = lastInMonth(
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(100)
        );
        assertEquals(1, size(resultingPoints));

        assertEquals(value(99, "measurementA", points), value(0, "measurementA", resultingPoints));
        assertEquals(value(99, "measurementB", points), value(0, "measurementB", resultingPoints));

        assertEquals(truncate(now, MONTHS).toInstant(), timestamp(0, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesOverMoreThanOneMonthWhenQueryingForMonthSnapshotsThenLastPointInEveryMonthAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), minutes, 100, "measurementA", "measurementB");
        indexPoints(minutes, points);
        List<TimeSeriesPoint> pointsMonth2 = createRandomTimeSeries(now.truncatedTo(DAYS).plusMonths(1), minutes, 100, "measurementA", "measurementB");
        indexPoints(minutes, pointsMonth2);

        List<TimeSeriesPoint> resultingPoints = lastInMonth(
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMonths(1).plusMinutes(100)
        );

        assertEquals(2, size(resultingPoints));

        assertEquals(value(99, "measurementA", points), value(0, "measurementA", resultingPoints));
        assertEquals(value(99, "measurementB", points), value(0, "measurementB", resultingPoints));
        assertEquals(value(99, "measurementA", pointsMonth2), value(1, "measurementA", resultingPoints));
        assertEquals(value(99, "measurementB", pointsMonth2), value(1, "measurementB", resultingPoints));

        assertEquals(truncate(now, MONTHS).toInstant(), timestamp(0, resultingPoints).toInstant());
        assertEquals(truncate(now.plusMonths(1), MONTHS).toInstant(), timestamp(1, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForDayPointsThenSummarizedMinutesAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), minutes, 100, "measurementA", "measurementB");
        indexPoints(minutes, points);
        List<TimeSeriesPoint> resultingPoints = request(
                days,
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(100)
        );
        assertEquals(1, size(resultingPoints));
        assertEquals(DataOperations.sum("measurementA", points), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementB", points), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(truncate(now, ChronoUnit.DAYS).toInstant(), timestamp(0, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForDayPointsForSeveralDaysThenSummarizedMinutesForEachDayAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), minutes, 2000, "measurementA", "measurementB");
        indexPoints(minutes, points);
        List<TimeSeriesPoint> resultingPoints = request(
                days,
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(2000)
        );

        List<TimeSeriesPoint> pointsDayOne = points.subList(0,1440);
        List<TimeSeriesPoint> pointsDayTwo = points.subList(1440, points.size());
        assertEquals(2, size(resultingPoints));
        assertEquals(DataOperations.sum("measurementA", pointsDayOne), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementA", pointsDayTwo), resultingPoints.get(1).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementB", pointsDayOne), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementB", pointsDayTwo), resultingPoints.get(1).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());

        assertEquals(DataOperations.sum("measurementA", points), DataOperations.sum("measurementA", resultingPoints));
        assertEquals(DataOperations.sum("measurementB", points), DataOperations.sum("measurementB", resultingPoints));
        assertEquals(truncate(now, ChronoUnit.DAYS).toInstant(), timestamp(0, resultingPoints).toInstant());
        assertEquals(truncate(now.plusDays(1), ChronoUnit.DAYS).toInstant(), timestamp(1, resultingPoints).toInstant());
    }

    @Test
    public void givenRandomMinuteSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenRandomSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned(minutes);
    }

    @Test
    public void givenRandomHourSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenRandomSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned(hours);
    }

    @Test
    public void givenRandomDaysSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenRandomSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned(days);
    }

    @Test
    public void givenRandomMonthsSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenRandomSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned(months);
    }

    @Test
    public void givenRandomYearSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenRandomSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned(years);
    }

    private void givenRandomSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned(MeasurementDistance distance) throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), distance, 78, "measurementA", "measurementB");
        indexPoints(distance, points);
        TimeSeriesPoint resultingPoint = requestSum(
                timeSeriesName,
                distance,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plus(100, unit(distance))
        );
        assertNotNull(resultingPoint);
        assertEquals(DataOperations.sum("measurementA", points), measurementValue("measurementA", resultingPoint));
        assertEquals(DataOperations.sum("measurementB", points), measurementValue("measurementB", resultingPoint));
        assertEquals(now.truncatedTo(DAYS).toInstant(), resultingPoint.getTimestamp().toInstant());
    }

    @Test
    public void givenFixedMinuteSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(minutes);
    }

    @Test
    public void givenFixedHourSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(hours);
    }

    @Test
    public void givenFixedDaySeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(days);
    }

    @Test
    public void givenFixedMonthSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(months);
    }

    @Test
    public void givenFixedYearSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(years);
    }

    private void givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(MeasurementDistance distance) throws IOException {
        // Note that sum which gives larger numbers than 2^52 will be inaccurate, as Elasticsearch uses doubles to avoid
        // overflow.
        List<TimeSeriesPoint> points = indexPointsFrom(now.truncatedTo(DAYS), distance, (long)Math.pow(2, 51), (long)Math.pow(2, 51)+121);
        TimeSeriesPoint resultingPoint = requestSum(
                timeSeriesName,
                distance,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plus(100, unit(distance))
        );
        assertNotNull(resultingPoint);
        assertEquals(DataOperations.sum(measurementId, points), measurementValue(measurementId, resultingPoint));
        assertEquals(now.truncatedTo(DAYS).toInstant(), resultingPoint.getTimestamp().toInstant());
    }

    private List<String> availableTimeSeries(String owner) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/minutes",
                HttpMethod.GET,
                null,
                String.class,
                owner
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<String>>(){}).readValue(response.getBody());
    }

    private TimeSeriesPoint last(String seriesName, String owner, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/minutes/last?from={from}&to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(TimeSeriesPoint.class).readValue(response.getBody());
    }

    private TimeSeriesPoint requestSum(String seriesName, MeasurementDistance distance, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}/sum?from={from}&to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance,
                formatTimestamp(from),
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(TimeSeriesPoint.class).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> request(MeasurementDistance distance, String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}?from={from}&to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance,
                formatTimestamp(from),
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> requestTo(MeasurementDistance distance, String seriesName, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}?to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance,
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> requestFrom(MeasurementDistance distance, String seriesName, ZonedDateTime from) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}?from={from}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance,
                formatTimestamp(from)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> request(MeasurementDistance distance, String seriesName) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance
                );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> lastInMonth(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/months/last?from={from}&to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> years(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/years?from={from}&to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }


    private List<TimeSeriesPoint> minutesAbovePercentile(int percentile, String measurementId, String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/minutes?from={from}&to={to}",
                HttpMethod.POST,
                new HttpEntity<>(new TimeSeriesFilter(percentile, measurementId)),
                String.class,
                owner,
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private void indexPoints(MeasurementDistance distance, List<TimeSeriesPoint> points) throws IOException {
        for (TimeSeriesPoint point : points)
            indexPoint(indexNameForSeries(timeSeriesName, distance, point.getTimestamp()), point);
    }

    private List<TimeSeriesPoint> indexPointsFrom(ZonedDateTime timestamp, MeasurementDistance distance, long... values) throws IOException {
        List<TimeSeriesPoint> points = new ArrayList<>(values.length);
        for (long value : values) {
            points.add(indexPoint(indexNameForSeries(timeSeriesName, distance, timestamp), timestamp, value));
            timestamp = timestamp.plus(1, unit(distance));
        }
        return points;
    }

    private TimeSeriesPoint indexPoint(String seriesName, MeasurementDistance distance, ZonedDateTime timestamp, long value) throws IOException {
        return indexPoint(indexNameForSeries(seriesName, distance, timestamp), timestamp, value);
    }

    private TimeSeriesPoint indexPoint(MeasurementDistance distance, ZonedDateTime timestamp, long value) throws IOException {
        return indexPoint(timeSeriesName, distance, timestamp, value);
    }

    private void indexPoint(String indexName, TimeSeriesPoint point) throws IOException {
        XContentBuilder sourceBuilder = jsonBuilder().startObject()
                .field("timestamp", formatTimestamp(point.getTimestamp()));
        for (Measurement measurement : point.getMeasurements())
            sourceBuilder.field(measurement.getId(), measurement.getValue());
        elasticsearchHelper.index(indexName, "default", sourceBuilder.endObject().string());
    }

    private TimeSeriesPoint indexPoint(String indexName, ZonedDateTime timestamp, long value) throws IOException {
        TimeSeriesPoint point = TimeSeriesPoint.builder().timestamp(timestamp).measurement(measurementId, value).build();
        indexPoint(indexName, point);
        return point;
    }

    private String indexNameForSeries(String baseName, MeasurementDistance distance, ZonedDateTime timestamp) {
        return resolveIndexName().seriesName(baseName).owner(owner).distance(distance).at(timestamp).single();
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

}
