package no.difi.statistics;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.config.AppConfig;
import no.difi.statistics.query.elasticsearch.config.ElasticsearchConfig;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.time.temporal.ChronoUnit.DAYS;
import static no.difi.statistics.test.utils.DataGenerator.createRandomTimeSeries;
import static no.difi.statistics.test.utils.DataOperations.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(
        webEnvironment = RANDOM_PORT
)
@ContextConfiguration(classes = {AppConfig.class, ElasticsearchConfig.class}, initializers = ElasticsearchQueryServiceTest.Initializer.class)
@RunWith(SpringRunner.class)
public class ElasticsearchQueryServiceTest {

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            GenericContainer backend = new GenericContainer("elasticsearch:2.3.5");
            backend.start();
            EnvironmentTestUtils.addEnvironment(
                    applicationContext.getEnvironment(),
                    "no.difi.statistics.elasticsearch.host=" + backend.getContainerIpAddress(),
                    "no.difi.statistics.elasticsearch.port=" + backend.getMappedPort(9300)
            );
            ElasticsearchQueryServiceTest.backend = backend;
        }

    }

    private ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 13, 30, 31, 123, ZoneId.of("UTC"));
    private final static String measurementId = "count";
    private static final String databaseName = "default";
    private final static String timeSeriesName = "test";

    private static GenericContainer backend;

    @Autowired
    private Client client;
    @Autowired
    private TestRestTemplate restTemplate;

    @Before
    public void prepare() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            if (((TransportClient)client).connectedNodes().size() > 0) break;
            Thread.sleep(10L);
        }
    }

    @After
    public void cleanup() throws ExecutionException, InterruptedException {
        client.admin().indices().prepareDelete("_all").get();
    }

    @AfterClass
    public static void cleanupAll() {
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
        assertPercentileTDigest(92, points, measurementId, resultingPoints);
    }

    @Test // 3, 5, 11, 13, 56, 234, 235, 546, 566, 574, 674, 777, 1244, 3454, 3455, 5667, 9000, 547547
    public void givenMinuteSeriesWithSize18WhenQueryingForDataPointsWithMeasurementAbove35thPercentileThenLargestMeasurementsFromPosition6AreReturned() throws IOException {
        int[] points = {13, 11, 546, 234, 3455, 547547, 574, 3, 3454, 5, 1244, 674, 566, 5667, 56, 777, 235, 9000};
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
        assertEquals(sum("measurementA", points), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).intValue());
        assertEquals(sum("measurementB", points), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).intValue());
        assertEquals(truncate(now, ChronoUnit.MONTHS).toInstant(), timestamp(0, resultingPoints).toInstant());
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
        assertEquals(sum("measurementA", points), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).intValue());
        assertEquals(sum("measurementB", points), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).intValue());
        assertEquals(truncate(now, ChronoUnit.DAYS).toInstant(), timestamp(0, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForPeriodPointThenSingleSummarizedPointIsReturned() throws IOException {
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

    private TimeSeriesPoint point(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return restTemplate.exchange(
                "/point/{seriesName}?from={from}&to={to}",
                HttpMethod.GET,
                null,
                TimeSeriesPoint.class,
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        ).getBody();
    }

    private List<TimeSeriesPoint> minutes(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return restTemplate.exchange(
                "/minutes/{seriesName}?from={from}&to={to}",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<TimeSeriesPoint>>(){},
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        ).getBody();
    }

    private List<TimeSeriesPoint> hours(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return restTemplate.exchange(
                "/hours/{seriesName}?from={from}&to={to}",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<TimeSeriesPoint>>(){},
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        ).getBody();
    }

    private List<TimeSeriesPoint> days(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return restTemplate.exchange(
                "/days/{seriesName}?from={from}&to={to}",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<TimeSeriesPoint>>(){},
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        ).getBody();
    }

    private List<TimeSeriesPoint> months(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return restTemplate.exchange(
                "/months/{seriesName}?from={from}&to={to}",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<TimeSeriesPoint>>(){},
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        ).getBody();
    }

    private List<TimeSeriesPoint> years(String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return restTemplate.exchange(
                "/years/{seriesName}?from={from}&to={to}",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<TimeSeriesPoint>>(){},
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        ).getBody();
    }


    private List<TimeSeriesPoint> minutesAbovePercentile(int percentile, String measurementId, String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        return restTemplate.exchange(
                "/minutes/{seriesName}?from={from}&to={to}",
                HttpMethod.POST,
                new HttpEntity<>(new TimeSeriesFilter(percentile, measurementId)),
                new ParameterizedTypeReference<List<TimeSeriesPoint>>(){},
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        ).getBody();
    }

    private void indexMinutePoints(List<TimeSeriesPoint> minutePoints) throws IOException {
        for (TimeSeriesPoint point : minutePoints)
            indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, point.getTimestamp()), point);
    }

    private void indexHourPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForHourSeries(timeSeriesName, timestamp), timestamp, value);
    }

    private void indexDayPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForDaySeries(timeSeriesName, timestamp), timestamp, value);
    }

    private void indexMonthPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForMonthSeries(timeSeriesName, timestamp), timestamp, value);
    }

    private void indexMinutePointsFrom(ZonedDateTime timestamp, int... values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, timestamp), timestamp, value);
            timestamp = timestamp.plusMinutes(1);
        }
    }

    private void indexHourPointsFrom(ZonedDateTime timestamp, int... values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForHourSeries(timeSeriesName, timestamp), timestamp, value);
            timestamp = timestamp.plusHours(1);
        }
    }

    private void indexDayPointsFrom(ZonedDateTime timestamp, int... values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForDaySeries(timeSeriesName, timestamp), timestamp, value);
            timestamp = timestamp.plusDays(1);
        }
    }

    private void indexMonthPointsFrom(ZonedDateTime timestamp, int... values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForMonthSeries(timeSeriesName, timestamp), timestamp, value);
            timestamp = timestamp.plusMonths(1);
        }
    }

    private void indexYearPointsFrom(ZonedDateTime timestamp, int... values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForYearSeries(timeSeriesName, timestamp), timestamp, value);
            timestamp = timestamp.plusYears(1);
        }
    }

    private void indexMinutePoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, timestamp), timestamp, value);
    }

    private void indexTimeSeriesPoint(String indexName, TimeSeriesPoint point) throws IOException {
        XContentBuilder sourceBuilder = jsonBuilder().startObject()
                .field("timestamp", formatTimestamp(point.getTimestamp()));
        for (Measurement measurement : point.getMeasurements())
            sourceBuilder.field(measurement.getId(), measurement.getValue());
        client.prepareIndex(indexName, "default")
                .setSource(sourceBuilder.endObject())
                .setRefresh(true) // Make document immediately searchable for the purpose of this test
                .get();
    }

    private void indexTimeSeriesPoint(String indexName, ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexName, TimeSeriesPoint.builder().timestamp(timestamp).measurement(measurementId, value).build());
    }

    private String indexNameForMinuteSeries(String baseName, ZonedDateTime timestamp) {
        return String.format(
                "%s:minute%s",
                baseName,
                DateTimeFormatter.ofPattern("yyyy.MM.dd").format(timestamp)
        );
    }

    private String indexNameForHourSeries(String baseName, ZonedDateTime timestamp) {
        return String.format(
                "%s:hour%s",
                baseName,
                DateTimeFormatter.ofPattern("yyyy.MM.dd").format(timestamp)
        );
    }

    private String indexNameForDaySeries(String baseName, ZonedDateTime timestamp) {
        return String.format(
                "%s:day%s",
                baseName,
                DateTimeFormatter.ofPattern("yyyy").format(timestamp)
        );
    }

    private String indexNameForMonthSeries(String baseName, ZonedDateTime timestamp) {
        return String.format(
                "%s:month%s",
                baseName,
                DateTimeFormatter.ofPattern("yyyy").format(timestamp)
        );
    }

    private String indexNameForYearSeries(String baseName, ZonedDateTime timestamp) {
        return String.format(
                "%s:year",
                baseName
        );
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }


}
