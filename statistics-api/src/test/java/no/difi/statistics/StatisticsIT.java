package no.difi.statistics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.restassured.response.Response;
import no.difi.statistics.helper.DockerHelper;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StatisticsIT {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final static String timeSeriesName = "test";
    private final static String measurementId = "count";
    private ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 13, 30, 31, 123, ZoneId.of("UTC"));
    private static Client elasticsearchClient;
    private static String apiBaseUrl;
    private static ObjectMapper objectMapper;

    @BeforeClass
    public static void initAll() throws UnknownHostException {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        DockerHelper dockerHelper = new DockerHelper();
        elasticsearchClient = elasticSearchClient(
                dockerHelper.address(),
                dockerHelper.portFor(9300, "elasticsearch")
        );
        apiBaseUrl = format(
                "http://%s:%s",
                dockerHelper.address(),
                dockerHelper.portFor(8080, "statistics-api")
        );
    }

    @After
    public void cleanup() throws ExecutionException, InterruptedException {
        elasticsearchClient.admin().indices().prepareDelete("_all").get();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexMinutePointsFrom(now.minusMinutes(1003), 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = minutes(timeSeriesName, now.minusMinutes(1002), now.minusMinutes(1001));
        assertEquals(1002, measurementValue(0, timeSeries));
        assertEquals(1001, measurementValue(1, timeSeries));
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
        assertEquals(1002, measurementValue(0, timeSeries));
        assertEquals(1001, measurementValue(1, timeSeries));
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
        assertEquals(1002, measurementValue(0, timeSeries));
        assertEquals(1001, measurementValue(1, timeSeries));
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
        assertEquals(1002, measurementValue(0, timeSeries));
        assertEquals(1001, measurementValue(1, timeSeries));
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
        assertEquals(3, measurementValue(0, timeSeries));
        assertEquals(2, measurementValue(1, timeSeries));
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

    @Test // 1, 2, 3, 4, 5, 8, 21, 44, 55, 89, 131, 700, 1000
    public void givenMinuteSeriesWithSize14WhenQueryingForDataPointsWithMeasurementAbove92ndPercentileThenLargestMeasurementsFromPosition13AreReturned() throws IOException {
        int[] points = {1000, 4, 700, 1, 2, 3, 5, 44, 8, 21, 200, 131, 55, 89};
        indexMinutePointsFrom(now.minusMinutes(14), points);
        List<TimeSeriesPoint> resultingPoints = minutesAbovePercentile(
                92, measurementId, timeSeriesName,
                now.minusMinutes(100), now.minusMinutes(0)
        );
        assertPercentile(92, points, resultingPoints);
    }

    @Test // 3, 5, 11, 13, 56, 234, 235, 546, 566, 574, 674, 777, 1244, 3454, 3455, 5667, 9000, 547547
    public void givenMinuteSeriesWithSize18WhenQueryingForDataPointsWithMeasurementAbove35thPercentileThenLargestMeasurementsFromPosition6AreReturned() throws IOException {
        int[] points = {13, 11, 546, 234, 3455, 547547, 574, 3, 3454, 5, 1244, 674, 566, 5667, 56, 777, 235, 9000};
        indexMinutePointsFrom(now.minusMinutes(300), points);
        List<TimeSeriesPoint> resultingPoints = minutesAbovePercentile(
                40, measurementId, timeSeriesName,
                now.minusMinutes(301), now.minusMinutes(0)
        );
        assertPercentile(40, points, resultingPoints);
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForMonthPointsThenSummarizedMinutesAreReturned() throws IOException {
        int[] points = {13, 11, 546, 234, 3455, 547547, 574, 3, 3454, 5, 1244, 674, 566, 5667, 56, 777, 235, 9000};
        indexMinutePointsFrom(now.truncatedTo(DAYS), points);
        List<TimeSeriesPoint> resultingPoints = months(
                timeSeriesName,
                now.truncatedTo(DAYS), now.truncatedTo(DAYS).plusDays(1).minusMinutes(1)
        );
        assertEquals(1, size(resultingPoints));
        assertEquals(IntStream.of(points).sum(), measurementValue(0, resultingPoints));
        assertEquals(truncate(now, ChronoUnit.MONTHS), timestamp(0, resultingPoints));
    }

    private void assertPercentile(int percent, int[] points, List<TimeSeriesPoint> resultingPoints) {
        int index = new BigDecimal(percent).multiply(new BigDecimal(points.length))
                .divide(new BigDecimal(100)).round(new MathContext(0, RoundingMode.UP)).intValue();
        int expectedPercentile = sort(points)[index-1];
        assertEquals(points.length - index, resultingPoints.size());
        for (TimeSeriesPoint point : resultingPoints) {
            assertThat(measurementValue(point), greaterThanOrEqualTo(expectedPercentile));
        }
    }

    private static ZonedDateTime truncate(ZonedDateTime timestamp, ChronoUnit toUnit) {
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

    private static int[] sort(int[] src) {
        int[] dst = src.clone();
        Arrays.sort(dst);
        return dst;
    }

    private static ZonedDateTime timestamp(int i, List<TimeSeriesPoint> timeSeries) throws IOException {
        return timeSeries.get(i).getTimestamp();
    }

    private static int measurementValue(int i, List<TimeSeriesPoint> timeSeries) throws IOException {
        return measurementValue(timeSeries.get(i));
    }

    private static int measurementValue(TimeSeriesPoint point) {
        return point.getMeasurement(measurementId).map(Measurement::getValue).orElseThrow(RuntimeException::new);
    }

    private static int size(List<TimeSeriesPoint> timeSeries) throws IOException {
        return timeSeries.size();
    }

    private List<TimeSeriesPoint> parseJson(String json) throws IOException {
        return objectMapper.readValue(json, new TypeReference<List<TimeSeriesPoint>>(){});
    }

    private void indexMinutePointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, timestamp), "total", timestamp, value);
            timestamp = timestamp.plusMinutes(1);
        }
    }

    private void indexHourPointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForHourSeries(timeSeriesName, timestamp), "total", timestamp, value);
            timestamp = timestamp.plusHours(1);
        }
    }

    private void indexDayPointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForDaySeries(timeSeriesName, timestamp), "total", timestamp, value);
            timestamp = timestamp.plusDays(1);
        }
    }

    private void indexMonthPointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForMonthSeries(timeSeriesName, timestamp), "total", timestamp, value);
            timestamp = timestamp.plusMonths(1);
        }
    }

    private void indexYearPointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForYearSeries(timeSeriesName, timestamp), "total", timestamp, value);
            timestamp = timestamp.plusYears(1);
        }
    }

    private void indexMinutePoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    private void indexHourPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForHourSeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    private void indexDayPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForDaySeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    private void indexMonthPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForMonthSeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    private void indexTimeSeriesPoint(String indexName, String type, ZonedDateTime timestamp, int value) throws IOException {
        logger.info(format(
                "Executing indexing:\nIndex: %s\nType: %s\nTimestamp: %s\nValue: %d",
                indexName,
                type,
                timestamp,
                value
        ));
        elasticsearchClient.prepareIndex(indexName, type)
                .setSource(
                        jsonBuilder().startObject()
                                .field("timestamp", formatTimestamp(timestamp))
                                .field(measurementId, value)
                                .endObject()
                )
                .setRefresh(true) // Make document immediately searchable for the purpose of this test
                .get();
    }

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

    private static Client elasticSearchClient(String host, int port) throws UnknownHostException {
        return TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    }


}
