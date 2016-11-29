package no.difi.statistics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.query.config.AppConfig;
import no.difi.statistics.query.elasticsearch.config.ElasticsearchConfig;
import no.difi.statistics.test.utils.DataOperations;
import no.difi.statistics.test.utils.ElasticsearchHelper;
import org.elasticsearch.client.Client;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.iterate;
import static no.difi.statistics.elasticsearch.Timestamp.truncate;
import static no.difi.statistics.model.MeasurementDistance.*;
import static no.difi.statistics.test.utils.DataGenerator.createRandomTimeSeries;
import static no.difi.statistics.test.utils.DataOperations.*;
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
    private final static String series = "test";
    private final static String anotherTimeSeriesName = "anothertimeseriesname";
    private final static String owner = "test_owner"; // Index names must be lower case in Elasticsearch

    private static GenericContainer backend;

    @Autowired
    private Client client;
    @Autowired
    private TestRestTemplate restTemplate;
    private ElasticsearchHelper helper;
    @Autowired
    private ObjectMapper objectMapper;

    @Before
    public void prepare() throws Exception {
        helper = new ElasticsearchHelper(
                client,
                backend.getContainerIpAddress(),
                backend.getMappedPort(9200)
        );
        helper.waitForGreenStatus();
    }

    @After
    public void cleanup() {
        helper.clear();
    }

    @AfterClass
    public static void cleanupAll() {
        backend.stop();
    }

    @Test
    public void givenTimeSeriesWhenQueryingForAvailableTimeSeriesThenAvailableTimeSeriesAreReturned() throws IOException, ExecutionException, InterruptedException {
        helper.indexPoint(series, minutes, now.minusMinutes(2), 1002);
        helper.indexPoint(series, minutes, now, 1003);
        helper.indexPoint(anotherTimeSeriesName, minutes, now, 42);
        List<String> availableTimeSeries = availableTimeSeries(owner);
        assertEquals(2, availableTimeSeries.size());
        assertTrue(availableTimeSeries.contains(series));
        assertTrue(availableTimeSeries.contains(anotherTimeSeriesName));
    }

    @Test
    public void givenTimeSeriesWhenQueryingForAvailableTimeSeriesWithAnotherOwnerThenNoTimeSeriesAreReturned() throws IOException, ExecutionException, InterruptedException {
        helper.indexPoint(series, minutes, now.minusMinutes(2), 1002);
        helper.indexPoint(series, minutes, now, 1003);
        helper.indexPoint(anotherTimeSeriesName, minutes, now, 42);
        List<String> availableTimeSeries = availableTimeSeries("anotherOwner");
        assertEquals(0, availableTimeSeries.size());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(minutes);
    }

    @Test
    public void givenHourSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(hours);
    }

    @Test
    public void givenDaySeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(days);
    }

    @Test
    public void givenMonthSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(months);
    }

    @Test
    public void givenYearSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(years);
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(minutes);
    }

    @Test
    public void givenHourSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(hours);
    }

    @Test
    public void givenDaySeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(days);
    }

    @Test
    public void givenMonthSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(months);
    }

    @Test
    public void givenYearSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(years);
    }

    private void givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(MeasurementDistance distance) throws IOException, ExecutionException, InterruptedException {
        helper.indexPointsFrom(dateTime(2003, 1, 1, 11, 11), distance, 1, 2, 3, 4, 5, 6, 7, 8, 9); // Some random "old" points
        TimeSeriesPoint expectedLastPoint = helper.indexPoint(distance, dateTime(2016, 3, 3, 12, 12), 123L);
        helper.indexPoint(distance, dateTime(2017, 4, 4, 1, 2), 5675L); // Point after last
        TimeSeriesPoint actualLastPoint = requestLast(series, distance, owner, dateTime(2007, 1, 1, 0, 0), dateTime(2016, 3, 3, 13, 0));
        assertEquals(expectedLastPoint, actualLastPoint);
    }

    private void givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(MeasurementDistance distance) throws IOException, ExecutionException, InterruptedException {
        helper.indexPointsFrom(dateTime(2003, 1, 1, 11, 11), distance, 1, 2, 3, 4, 5, 6, 7, 8, 9); // Some random "old" points
        TimeSeriesPoint expectedLastPoint = helper.indexPoint(distance, dateTime(2016, 3, 3, 12, 12), 123L);
        TimeSeriesPoint actualLastPoint = requestLast(series, distance, owner);
        assertEquals(expectedLastPoint, actualLastPoint);
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusMinutes(1003), minutes, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(minutes, series, now.minusMinutes(1002), now.minusMinutes(1001));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMinuteSeriesLastingTwoDaysWhenQueryingForRangeOverThoseDaysThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPoint(minutes, now.minusDays(1), 13);
        helper.indexPoint(minutes, now, 117);
        List<TimeSeriesPoint> timeSeries = request(minutes, series, now.minusDays(1).minusHours(1), now);
        assertEquals(13, measurementValue(measurementId, 0, timeSeries));
        assertEquals(117, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusMinutes(20), minutes, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(minutes, series, now.minusMinutes(9), now.minusMinutes(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPoint(minutes, now.minusMinutes(100), 100);
        helper.indexPoint(minutes, now.minusMinutes(200), 200);
        helper.indexPoint(minutes, now.minusMinutes(300), 300);
        helper.indexPoint(minutes, now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = request(minutes, series, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithoutRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPoint(minutes, now.minusMinutes(100), 100);
        helper.indexPoint(minutes, now.minusMinutes(200), 200);
        helper.indexPoint(minutes, now.minusMinutes(300), 300);
        helper.indexPoint(minutes, now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = request(minutes, series);
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithLeftOpenRangeThenCorrectDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPoint(minutes, now.minusMinutes(100), 100);
        helper.indexPoint(minutes, now.minusMinutes(200), 200);
        helper.indexPoint(minutes, now.minusMinutes(300), 300);
        helper.indexPoint(minutes, now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = requestTo(minutes, series, now.minusMinutes(101));
        assertEquals(3, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithRightOpenRangeThenCorrectDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPoint(minutes, now.minusMinutes(100), 100);
        helper.indexPoint(minutes, now.minusMinutes(200), 200);
        helper.indexPoint(minutes, now.minusMinutes(300), 300);
        helper.indexPoint(minutes, now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = requestFrom(minutes, series, now.minusMinutes(101));
        assertEquals(1, size(timeSeries));
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusHours(1003), hours, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(hours, series, now.minusHours(1002), now.minusHours(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusHours(20), hours, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(hours, series, now.minusHours(9), now.minusHours(8));
        assertEquals(0, size(timeSeries));
    }

    @Test @Ignore
    public void givenHourSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPoint(hours, now.minusHours(100), 100);
        helper.indexPoint(hours, now.minusHours(200), 200);
        helper.indexPoint(hours, now.minusHours(300), 300);
        helper.indexPoint(hours, now.minusHours(400), 400);
        List<TimeSeriesPoint> timeSeries = request(hours, series, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusDays(1003), days, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(days, series, now.minusDays(1002), now.minusDays(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusDays(20), days, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(days, series, now.minusDays(9), now.minusDays(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPoint(days, now.minusDays(100), 100);
        helper.indexPoint(days, now.minusDays(200), 200);
        helper.indexPoint(days, now.minusDays(300), 300);
        helper.indexPoint(days, now.minusDays(400), 400);
        List<TimeSeriesPoint> timeSeries = request(days, series, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusMonths(1003), months, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(months, series, now.minusMonths(1002), now.minusMonths(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusMonths(20), months, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(months, series, now.minusMonths(9), now.minusMonths(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPoint(months, now.minusMonths(10), 100);
        helper.indexPoint(months, now.minusMonths(20), 200);
        helper.indexPoint(months, now.minusMonths(30), 300);
        helper.indexPoint(months, now.minusMonths(40), 400);
        List<TimeSeriesPoint> timeSeries = request(months, series, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusYears(4), years, 4, 3, 2, 1);
        List<TimeSeriesPoint> timeSeries = request(years, series, now.minusYears(3), now.minusYears(2));
        assertEquals(2, size(timeSeries));
        assertEquals(3, measurementValue(measurementId, 0, timeSeries));
        assertEquals(2, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusYears(20), years, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(years, series, now.minusYears(9), now.minusYears(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        helper.indexPointsFrom(now.minusYears(4), years, 4, 3, 2, 1);
        List<TimeSeriesPoint> timeSeries = request(years, series, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() throws IOException {
        givenSeries("series1").withDistance(minutes).withMeasurements("m1", "m2", "m3", "m4").end()
            .when().request(points().from("series1").withDistance(minutes).withMeasurement("m1").lessThanPercentile(92))
                .then().pointsAreReturned(lessThanPercentile(92).forMeasurement("m1"))
            .when().request(points().from("series1").withDistance(minutes).withMeasurement("m2").greaterThanPercentile(43))
                .then().pointsAreReturned(greaterThanPercentile(43).forMeasurement("m2"))
            .when().request(points().from("series1").withDistance(minutes).withMeasurement("m3").lessThanOrEqualToPercentile(15))
                .then().pointsAreReturned(lessThanOrEqualToPercentile(15).forMeasurement("m3"))
            .when().request(points().from("series1").withDistance(minutes).withMeasurement("m4").greaterThanOrEqualToPercentile(57))
                .then().pointsAreReturned(greaterThanOrEqualToPercentile(57).forMeasurement("m4"));
    }

    @Test
    public void givenHourSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() throws IOException {
        givenSeries("series2").withDistance(hours).withMeasurements("m1", "m2", "m3", "m4").end()
            .when().request(points().from("series2").withDistance(hours).withMeasurement("m1").lessThanPercentile(92))
                .then().pointsAreReturned(lessThanPercentile(92).forMeasurement("m1"))
            .when().request(points().from("series2").withDistance(hours).withMeasurement("m2").greaterThanPercentile(43))
                .then().pointsAreReturned(greaterThanPercentile(43).forMeasurement("m2"))
            .when().request(points().from("series2").withDistance(hours).withMeasurement("m3").lessThanOrEqualToPercentile(15))
                .then().pointsAreReturned(lessThanOrEqualToPercentile(15).forMeasurement("m3"))
            .when().request(points().from("series2").withDistance(hours).withMeasurement("m4").greaterThanOrEqualToPercentile(57))
                .then().pointsAreReturned(greaterThanOrEqualToPercentile(57).forMeasurement("m4"));
    }

    @Test
    public void givenDaySeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() throws IOException {
        givenSeries("series3").withDistance(days).withMeasurements("m1", "m2", "m3", "m4").end()
            .when().request(points().from("series3").withDistance(days).withMeasurement("m1").lessThanPercentile(92))
                .then().pointsAreReturned(lessThanPercentile(92).forMeasurement("m1"))
            .when().request(points().from("series3").withDistance(days).withMeasurement("m2").greaterThanPercentile(43))
                .then().pointsAreReturned(greaterThanPercentile(43).forMeasurement("m2"))
            .when().request(points().from("series3").withDistance(days).withMeasurement("m3").lessThanOrEqualToPercentile(15))
                .then().pointsAreReturned(lessThanOrEqualToPercentile(15).forMeasurement("m3"))
            .when().request(points().from("series3").withDistance(days).withMeasurement("m4").greaterThanOrEqualToPercentile(57))
                .then().pointsAreReturned(greaterThanOrEqualToPercentile(57).forMeasurement("m4"));
    }

    @Test
    public void givenMonthSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() throws IOException {
        givenSeries("series4").withDistance(months).withMeasurements("m1", "m2", "m3", "m4").end()
            .when().request(points().from("series4").withDistance(months).withMeasurement("m1").lessThanPercentile(92))
                .then().pointsAreReturned(lessThanPercentile(92).forMeasurement("m1"))
            .when().request(points().from("series4").withDistance(months).withMeasurement("m2").greaterThanPercentile(43))
                .then().pointsAreReturned(greaterThanPercentile(43).forMeasurement("m2"))
            .when().request(points().from("series4").withDistance(months).withMeasurement("m3").lessThanOrEqualToPercentile(15))
                .then().pointsAreReturned(lessThanOrEqualToPercentile(15).forMeasurement("m3"))
            .when().request(points().from("series4").withDistance(months).withMeasurement("m4").greaterThanOrEqualToPercentile(57))
                .then().pointsAreReturned(greaterThanOrEqualToPercentile(57).forMeasurement("m4"));
    }

    @Test
    public void givenYearSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() throws IOException {
        givenSeries("series5").withDistance(years).withMeasurements("m1", "m2", "m3", "m4").end()
            .when().request(points().from("series5").withDistance(years).withMeasurement("m1").lessThanPercentile(92))
                .then().pointsAreReturned(lessThanPercentile(92).forMeasurement("m1"))
            .when().request(points().from("series5").withDistance(years).withMeasurement("m2").greaterThanPercentile(43))
                .then().pointsAreReturned(greaterThanPercentile(43).forMeasurement("m2"))
            .when().request(points().from("series5").withDistance(years).withMeasurement("m3").lessThanOrEqualToPercentile(15))
                .then().pointsAreReturned(lessThanOrEqualToPercentile(15).forMeasurement("m3"))
            .when().request(points().from("series5").withDistance(years).withMeasurement("m4").greaterThanOrEqualToPercentile(57))
                .then().pointsAreReturned(greaterThanOrEqualToPercentile(57).forMeasurement("m4"));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForMonthPointsThenSummarizedMinutesAreReturned() throws IOException {
        givenSeries(series).withDistance(minutes).from(now.truncatedTo(DAYS)).withMeasurements("m1", "m2").end()
            .when().request(points().from(series).withDistance(months).go())
                .then().pointsAreReturned(sum(months));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForMonthSnapshotsThenLastPointInMonthAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), minutes, 100, "measurementA", "measurementB");
        helper.indexPoints(minutes, points);
        List<TimeSeriesPoint> resultingPoints = points().from(series)
                .from(now.truncatedTo(DAYS))
                .to(now.truncatedTo(DAYS).plusMinutes(100))
                .lastInMonth().get();
        assertEquals(1, size(resultingPoints));

        assertEquals(value(99, "measurementA", points), value(0, "measurementA", resultingPoints));
        assertEquals(value(99, "measurementB", points), value(0, "measurementB", resultingPoints));

        assertEquals(truncate(now, MONTHS).toInstant(), timestamp(0, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesOverMoreThanOneMonthWhenQueryingForMonthSnapshotsThenLastPointInEveryMonthAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), minutes, 100, "measurementA", "measurementB");
        helper.indexPoints(minutes, points);
        List<TimeSeriesPoint> pointsMonth2 = createRandomTimeSeries(now.truncatedTo(DAYS).plusMonths(1), minutes, 100, "measurementA", "measurementB");
        helper.indexPoints(minutes, pointsMonth2);

        List<TimeSeriesPoint> resultingPoints = points().from(series)
                .from(now.truncatedTo(DAYS))
                .to(now.truncatedTo(DAYS).plusMonths(1).plusMinutes(100))
                .lastInMonth().get();

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
        helper.indexPoints(minutes, points);
        List<TimeSeriesPoint> resultingPoints = request(
                days,
                series,
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
        helper.indexPoints(minutes, points);
        List<TimeSeriesPoint> resultingPoints = request(
                days,
                series,
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
        helper.indexPoints(distance, points);
        TimeSeriesPoint resultingPoint = requestSum(
                series,
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
        List<TimeSeriesPoint> points = helper.indexPointsFrom(now.truncatedTo(DAYS), distance, (long)Math.pow(2, 51), (long)Math.pow(2, 51)+121);
        TimeSeriesPoint resultingPoint = requestSum(
                series,
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

    private TimeSeriesPoint requestLast(String seriesName, MeasurementDistance distance, String owner, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}/last?from={from}&to={to}",
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

    private TimeSeriesPoint requestLast(String seriesName, MeasurementDistance distance, String owner) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}/last",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance
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

    private List<TimeSeriesPoint> request(MeasurementDistance distance, String seriesName) {
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
        try {
            return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private RequestFunctionBuilder points() {
        return new RequestFunctionBuilder(restTemplate, objectMapper);
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

    private static ZonedDateTime dateTime(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, UTC);
    }

    private static ZonedDateTime year(int year) {
        return dateTime(year, 1, 1, 0, 0);
    }

    private Given givenSeries(String series) {
        return new Given(series, helper);
    }

    private static class Given {

        private String series;
        private ZonedDateTime from = year(1977);
        private int size = 100;
        private MeasurementDistance distance;
        private List<String> measurementIds;
        private List<TimeSeriesPoint> points;
        private ElasticsearchHelper helper;

        private Given(String series, ElasticsearchHelper helper) {
            this.series = series;
            this.helper = helper;
        }

        private Given from(ZonedDateTime from) {
            this.from = from;
            return this;
        }

        private Given size(int size) {
            this.size = size;
            return this;
        }

        Given withDistance(MeasurementDistance distance) {
            this.distance = distance;
            return this;
        }

        Given withMeasurements(String...ids) {
            this.measurementIds = asList(ids);
            return this;
        }

        Given end() {
            this.points = iterate(from, from -> from.plus(1, unit(distance)))
                    .map(t -> TimeSeriesPoint.builder().timestamp(t).measurements(randomMeasurements(measurementIds)).build())
                    .limit(size)
                    .collect(toList());
            try {
                helper.indexPoints(this.series, this.distance, this.points);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        private static Random random = new Random();

        private static List<Measurement> randomMeasurements(List<String> ids) {
            return ids.stream().map(id -> new Measurement(id, random.nextInt(1_000_000))).collect(toList());
        }

        When when() {
            return new When(this, this);
        }

    }

    private static class When {
        private Given given;
        private Supplier<List<TimeSeriesPoint>> requestFunction;
        private List<TimeSeriesPoint> resultingPoints;

        When(Given given, Given givenSeries) {
            this.given = given;
            this.given = givenSeries;
        }

        When request(Supplier<List<TimeSeriesPoint>> requestFunction) {
            this.requestFunction = requestFunction;
            return this;
        }

        Then then() {
            this.resultingPoints = requestFunction.get();
            return new Then(given, this);
        }
    }

    private static class Then {
        private Given given;
        private When when;

        Then(Given given, When when) {
            this.given = given;
            this.when = when;
        }

        Given pointsAreReturned(Function<List<TimeSeriesPoint>, List<TimeSeriesPoint>> verificationFunction) {
            List<TimeSeriesPoint> expectedPoints = verificationFunction.apply(given.points);
            assertEquals(expectedPoints, when.resultingPoints);
            return given;
        }

    }

}
