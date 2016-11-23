package no.difi.statistics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.elasticsearch.IndexNameResolver;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.config.AppConfig;
import no.difi.statistics.query.elasticsearch.config.ElasticsearchConfig;
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

import static java.time.temporal.ChronoUnit.DAYS;
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
        indexMinutePoint(timeSeriesName, now.minusMinutes(2), 1002);
        indexMinutePoint(timeSeriesName, now, 1003);
        indexMinutePoint(anotherTimeSeriesName, now, 42);
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
        indexMinutePointsFrom(dateTime(2007, 1, 1, 11, 11), 1, 2, 3, 4, 5, 6, 7, 8, 9); // Some random "old" points
        TimeSeriesPoint expectedLastPoint = indexMinutePoint(dateTime(2016, 3, 3, 12, 12), 123L);
        indexMinutePoint(dateTime(2016, 3, 4, 1, 2), 5675L);
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
        indexMinutePointsFrom(now.minusMinutes(1003), 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = minutes(timeSeriesName, now.minusMinutes(1002), now.minusMinutes(1001));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMinuteSeriesLastingTwoDaysWhenQueryingForRangeOverThoseDaysThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexMinutePoint(now.minusDays(1), 13);
        indexMinutePoint(now, 117);
        List<TimeSeriesPoint> timeSeries = minutes(timeSeriesName, now.minusDays(1).minusHours(1), now);
        assertEquals(13, measurementValue(measurementId, 0, timeSeries));
        assertEquals(117, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexMinutePointsFrom(now.minusMinutes(20), 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = minutes(timeSeriesName, now.minusMinutes(9), now.minusMinutes(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexMinutePoint(now.minusMinutes(100), 100);
        indexMinutePoint(now.minusMinutes(200), 200);
        indexMinutePoint(now.minusMinutes(300), 300);
        indexMinutePoint(now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = minutes(timeSeriesName, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithoutRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexMinutePoint(now.minusMinutes(100), 100);
        indexMinutePoint(now.minusMinutes(200), 200);
        indexMinutePoint(now.minusMinutes(300), 300);
        indexMinutePoint(now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = minutes(timeSeriesName);
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithLeftOpenRangeThenCorrectDataPointsAreReturned() throws IOException, InterruptedException {
        indexMinutePoint(now.minusMinutes(100), 100);
        indexMinutePoint(now.minusMinutes(200), 200);
        indexMinutePoint(now.minusMinutes(300), 300);
        indexMinutePoint(now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = minutesTo(timeSeriesName, now.minusMinutes(101));
        assertEquals(3, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithRightOpenRangeThenCorrectDataPointsAreReturned() throws IOException, InterruptedException {
        indexMinutePoint(now.minusMinutes(100), 100);
        indexMinutePoint(now.minusMinutes(200), 200);
        indexMinutePoint(now.minusMinutes(300), 300);
        indexMinutePoint(now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = minutesFrom(timeSeriesName, now.minusMinutes(101));
        assertEquals(1, size(timeSeries));
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

    @Test
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

    @Test
    public void givenMonthSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexMonthPoint(now.minusMonths(10), 100);
        indexMonthPoint(now.minusMonths(20), 200);
        indexMonthPoint(now.minusMonths(30), 300);
        indexMonthPoint(now.minusMonths(40), 400);
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
        long[] points = {1000, 4, 700, 1, 2, 3, 5, 44, 8, 21, 200, 131, 55, 89};
        indexMinutePointsFrom(now.minusMinutes(50), points);
        List<TimeSeriesPoint> resultingPoints = minutesAbovePercentile(
                92, measurementId, timeSeriesName,
                now.minusMinutes(100), now.minusMinutes(0)
        );
        assertPercentileTDigest(92, points, measurementId, resultingPoints);
    }

    @Test // 3, 5, 11, 13, 56, 234, 235, 546, 566, 574, 674, 777, 1244, 3454, 3455, 5667, 9000, 547547
    public void givenMinuteSeriesWithSize18WhenQueryingForDataPointsWithMeasurementAbove35thPercentileThenLargestMeasurementsFromPosition6AreReturned() throws IOException {
        long[] points = {13, 11, 546, 234, 3455, 547547, 574, 3, 3454, 5, 1244, 674, 566, 5667, 56, 777, 235, 9000};
        indexMinutePointsFrom(now.minusMinutes(300), points);
        List<TimeSeriesPoint> resultingPoints = minutesAbovePercentile(
                40, measurementId, timeSeriesName,
                now.minusMinutes(301), now.minusMinutes(0)
        );
        assertPercentileTDigest(40, points, measurementId, resultingPoints);
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForMonthPointsThenSummarizedMinutesAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), ChronoUnit.MINUTES, 100, "measurementA", "measurementB");
        indexMinutePoints(points);
        List<TimeSeriesPoint> resultingPoints = months(
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(100)
        );
        assertEquals(1, size(resultingPoints));
        assertEquals(sum("measurementA", points), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(sum("measurementB", points), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(truncate(now, ChronoUnit.MONTHS).toInstant(), timestamp(0, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForMonthSnapshotsThenLastPointInMonthAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), ChronoUnit.MINUTES, 100, "measurementA", "measurementB");
        indexMinutePoints(points);
        List<TimeSeriesPoint> resultingPoints = lastInMonth(
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(100)
        );
        assertEquals(1, size(resultingPoints));

        assertEquals(getValueFromTimeSeriesPointList(points, 99, "measurementA"), getValueFromTimeSeriesPointList(resultingPoints, 0, "measurementA"));
        assertEquals(getValueFromTimeSeriesPointList(points, 99, "measurementB"), getValueFromTimeSeriesPointList(resultingPoints, 0, "measurementB"));

        assertEquals(truncate(now, ChronoUnit.MONTHS).toInstant(), timestamp(0, resultingPoints).toInstant());
    }

    private long getValueFromTimeSeriesPointList(List<TimeSeriesPoint> timeSeriesPoints, int index, String measurementId){
        return timeSeriesPoints.get(index).getMeasurement(measurementId).map(Measurement::getValue).get();
    }

    @Test
    public void givenMinuteSeriesOverMoreThanOneMonthWhenQueryingForMonthSnapshotsThenLastPointInEveryMonthAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), ChronoUnit.MINUTES, 100, "measurementA", "measurementB");
        indexMinutePoints(points);
        List<TimeSeriesPoint> pointsMonth2 = createRandomTimeSeries(now.truncatedTo(DAYS).plusMonths(1), ChronoUnit.MINUTES, 100, "measurementA", "measurementB");
        indexMinutePoints(pointsMonth2);

        List<TimeSeriesPoint> resultingPoints = lastInMonth(
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMonths(1).plusMinutes(100)
        );

        assertEquals(2, size(resultingPoints));

        assertEquals(getValueFromTimeSeriesPointList(points, 99, "measurementA"), getValueFromTimeSeriesPointList(resultingPoints, 0, "measurementA"));
        assertEquals(getValueFromTimeSeriesPointList(points, 99, "measurementB"), getValueFromTimeSeriesPointList(resultingPoints, 0, "measurementB"));
        assertEquals(getValueFromTimeSeriesPointList(pointsMonth2, 99, "measurementA"), getValueFromTimeSeriesPointList(resultingPoints, 1, "measurementA"));
        assertEquals(getValueFromTimeSeriesPointList(pointsMonth2, 99, "measurementB"), getValueFromTimeSeriesPointList(resultingPoints, 1, "measurementB"));

        assertEquals(truncate(now, ChronoUnit.MONTHS).toInstant(), timestamp(0, resultingPoints).toInstant());
        assertEquals(truncate(now.plusMonths(1), ChronoUnit.MONTHS).toInstant(), timestamp(1, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForDayPointsThenSummarizedMinutesAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), ChronoUnit.MINUTES, 100, "measurementA", "measurementB");
        indexMinutePoints(points);
        List<TimeSeriesPoint> resultingPoints = days(
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(100)
        );
        assertEquals(1, size(resultingPoints));
        assertEquals(sum("measurementA", points), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(sum("measurementB", points), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(truncate(now, ChronoUnit.DAYS).toInstant(), timestamp(0, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForDayPointsForSeveralDaysThenSummarizedMinutesForEachDayAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), ChronoUnit.MINUTES, 2000, "measurementA", "measurementB");
        indexMinutePoints(points);
        List<TimeSeriesPoint> resultingPoints = days(
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(2000)
        );

        List<TimeSeriesPoint> pointsDayOne = points.subList(0,1440);
        List<TimeSeriesPoint> pointsDayTwo = points.subList(1440, points.size());
        assertEquals(2, size(resultingPoints));
        assertEquals(sum("measurementA", pointsDayOne), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(sum("measurementA", pointsDayTwo), resultingPoints.get(1).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(sum("measurementB", pointsDayOne), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(sum("measurementB", pointsDayTwo), resultingPoints.get(1).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());

        assertEquals(sum("measurementA", points), sum("measurementA", resultingPoints));
        assertEquals(sum("measurementB", points), sum("measurementB", resultingPoints));
        assertEquals(truncate(now, ChronoUnit.DAYS).toInstant(), timestamp(0, resultingPoints).toInstant());
        assertEquals(truncate(now.plusDays(1), ChronoUnit.DAYS).toInstant(), timestamp(1, resultingPoints).toInstant());
    }

    @Test
    public void givenRandomMinuteSeriesWhenQueryingForPeriodPointThenSingleSummarizedPointIsReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), ChronoUnit.MINUTES, 78, "measurementA", "measurementB");
        indexMinutePoints(points);
        TimeSeriesPoint resultingPoint = point(
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(100)
        );
        assertNotNull(resultingPoint);
        assertEquals(sum("measurementA", points), measurementValue("measurementA", resultingPoint));
        assertEquals(sum("measurementB", points), measurementValue("measurementB", resultingPoint));
        assertEquals(now.truncatedTo(DAYS).toInstant(), resultingPoint.getTimestamp().toInstant());
    }

    @Test
    public void givenFixedMinuteSeriesWhenQueryingForPeriodPointThenSingleSummarizedPointIsReturned() throws IOException {
        // Note that sum which gives larger numbers than 2^52 will be inaccurate, as Elasticsearch uses doubles to avoid
        // overflow.
        List<TimeSeriesPoint> points = indexMinutePointsFrom(now.truncatedTo(DAYS), (long)Math.pow(2, 51), (long)Math.pow(2, 51)+121);
        TimeSeriesPoint resultingPoint = point(
                timeSeriesName,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(100)
        );
        assertNotNull(resultingPoint);
        assertEquals(sum(measurementId, points), measurementValue(measurementId, resultingPoint));
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

    private TimeSeriesPoint point(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/point?from={from}&to={to}",
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

    private List<TimeSeriesPoint> minutes(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/minutes?from={from}&to={to}",
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

    private List<TimeSeriesPoint> minutesTo(String seriesName, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/minutes?to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> minutesFrom(String seriesName, ZonedDateTime from) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/minutes?from={from}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                formatTimestamp(from)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> minutes(String seriesName) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/minutes",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> hours(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/hours?from={from}&to={to}",
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

    private List<TimeSeriesPoint> days(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/days?from={from}&to={to}",
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

    private List<TimeSeriesPoint> months(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/months?from={from}&to={to}",
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

    private void indexMinutePoints(List<TimeSeriesPoint> minutePoints) throws IOException {
        for (TimeSeriesPoint point : minutePoints)
            indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, point.getTimestamp()), point);
    }

    private TimeSeriesPoint indexHourPoint(ZonedDateTime timestamp, long value) throws IOException {
        return indexTimeSeriesPoint(indexNameForHourSeries(timeSeriesName, timestamp), timestamp, value);
    }

    private TimeSeriesPoint indexDayPoint(ZonedDateTime timestamp, long value) throws IOException {
        return indexTimeSeriesPoint(indexNameForDaySeries(timeSeriesName, timestamp), timestamp, value);
    }

    private TimeSeriesPoint indexMonthPoint(ZonedDateTime timestamp, long value) throws IOException {
        return indexTimeSeriesPoint(indexNameForMonthSeries(timeSeriesName, timestamp), timestamp, value);
    }

    private List<TimeSeriesPoint> indexMinutePointsFrom(ZonedDateTime timestamp, long... values) throws IOException {
        List<TimeSeriesPoint> points = new ArrayList<>(values.length);
        for (long value : values) {
            points.add(indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, timestamp), timestamp, value));
            timestamp = timestamp.plusMinutes(1);
        }
        return points;
    }

    private List<TimeSeriesPoint> indexHourPointsFrom(ZonedDateTime timestamp, long... values) throws IOException {
        List<TimeSeriesPoint> points = new ArrayList<>(values.length);
        for (long value : values) {
            points.add(indexTimeSeriesPoint(indexNameForHourSeries(timeSeriesName, timestamp), timestamp, value));
            timestamp = timestamp.plusHours(1);
        }
        return points;
    }

    private List<TimeSeriesPoint> indexDayPointsFrom(ZonedDateTime timestamp, long... values) throws IOException {
        List<TimeSeriesPoint> points = new ArrayList<>(values.length);
        for (long value : values) {
            points.add(indexTimeSeriesPoint(indexNameForDaySeries(timeSeriesName, timestamp), timestamp, value));
            timestamp = timestamp.plusDays(1);
        }
        return points;
    }

    private List<TimeSeriesPoint> indexMonthPointsFrom(ZonedDateTime timestamp, long... values) throws IOException {
        List<TimeSeriesPoint> points = new ArrayList<>(values.length);
        for (long value : values) {
            points.add(indexTimeSeriesPoint(indexNameForMonthSeries(timeSeriesName, timestamp), timestamp, value));
            timestamp = timestamp.plusMonths(1);
        }
        return points;
    }


    private List<TimeSeriesPoint> indexYearPointsFrom(ZonedDateTime timestamp, long... values) throws IOException {
        List<TimeSeriesPoint> points = new ArrayList<>(values.length);
        for (long value : values) {
            points.add(indexTimeSeriesPoint(indexNameForYearSeries(timeSeriesName, timestamp), timestamp, value));
            timestamp = timestamp.plusYears(1);
        }
        return points;
    }

    private TimeSeriesPoint indexMinutePoint(String seriesName, ZonedDateTime timestamp, long value) throws IOException {
        return indexTimeSeriesPoint(indexNameForMinuteSeries(seriesName, timestamp), timestamp, value);
    }

    private TimeSeriesPoint indexMinutePoint(ZonedDateTime timestamp, long value) throws IOException {
        return indexMinutePoint(timeSeriesName, timestamp, value);
    }

    private void indexTimeSeriesPoint(String indexName, TimeSeriesPoint point) throws IOException {
        XContentBuilder sourceBuilder = jsonBuilder().startObject()
                .field("timestamp", formatTimestamp(point.getTimestamp()));
        for (Measurement measurement : point.getMeasurements())
            sourceBuilder.field(measurement.getId(), measurement.getValue());
        elasticsearchHelper.index(indexName, "default", sourceBuilder.endObject().string());
    }

    private TimeSeriesPoint indexTimeSeriesPoint(String indexName, ZonedDateTime timestamp, long value) throws IOException {
        TimeSeriesPoint point = TimeSeriesPoint.builder().timestamp(timestamp).measurement(measurementId, value).build();
        indexTimeSeriesPoint(indexName, point);
        return point;
    }

    private String indexNameForMinuteSeries(String baseName, ZonedDateTime timestamp) {
        return IndexNameResolver.resolveIndexName().seriesName(baseName).owner(owner).minutes().at(timestamp).single();
    }

    private String indexNameForHourSeries(String baseName, ZonedDateTime timestamp) {
        return IndexNameResolver.resolveIndexName().seriesName(baseName).owner(owner).hours().at(timestamp).single();
    }

    private String indexNameForDaySeries(String baseName, ZonedDateTime timestamp) {
        return IndexNameResolver.resolveIndexName().seriesName(baseName).owner(owner).days().at(timestamp).single();
    }

    private String indexNameForMonthSeries(String baseName, ZonedDateTime timestamp) {
        return IndexNameResolver.resolveIndexName().seriesName(baseName).owner(owner).months().at(timestamp).single();
    }

    private String indexNameForYearSeries(String baseName, ZonedDateTime timestamp) {
        return IndexNameResolver.resolveIndexName().seriesName(baseName).owner(owner).years().at(timestamp).single();
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }


}
