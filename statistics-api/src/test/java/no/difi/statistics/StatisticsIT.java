package no.difi.statistics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.restassured.RestAssured.get;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

public class StatisticsIT {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private String timeSeriesName = "test";
    private ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 13, 30, 31, 123, UTC);
    private static Client client;

    @BeforeClass
    public static void initAll() throws UnknownHostException {
        client = elasticSearchClient();
    }

    @After
    public void cleanup() throws ExecutionException, InterruptedException {
        client.admin().indices().prepareDelete("_all").get();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMinutePoint(now.minusMinutes(1000), 1000);
        indexTimeSeriesMinutePoint(now.minusMinutes(1001), 1001);
        indexTimeSeriesMinutePoint(now.minusMinutes(1002), 1002);
        indexTimeSeriesMinutePoint(now.minusMinutes(1003), 1003);
        List<TimeSeriesPoint> timeSeries = parseJson(minutes(timeSeriesName, now.minusMinutes(1002), now.minusMinutes(1001)));
        assertEquals(1002, value(0, timeSeries));
        assertEquals(1001, value(1, timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMinutePoint(now.minusMinutes(20), 20);
        indexTimeSeriesMinutePoint(now.minusMinutes(19), 19);
        indexTimeSeriesMinutePoint(now.minusMinutes(18), 18);
        indexTimeSeriesMinutePoint(now.minusMinutes(17), 17);
        List<TimeSeriesPoint> timeSeries = parseJson(minutes(timeSeriesName, now.minusMinutes(9), now.minusMinutes(8)));
        assertEquals(0, size(timeSeries));
    }

    @Test @Ignore
    public void givenMinuteSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMinutePoint(now.minusMinutes(100), 100);
        indexTimeSeriesMinutePoint(now.minusMinutes(200), 200);
        indexTimeSeriesMinutePoint(now.minusMinutes(300), 300);
        indexTimeSeriesMinutePoint(now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = parseJson(minutes(timeSeriesName, now.minusYears(10), now.plusYears(10)));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesHourPoint(now.minusHours(1000), 1000);
        indexTimeSeriesHourPoint(now.minusHours(1001), 1001);
        indexTimeSeriesHourPoint(now.minusHours(1002), 1002);
        indexTimeSeriesHourPoint(now.minusHours(1003), 1003);
        List<TimeSeriesPoint> timeSeries = parseJson(hours(timeSeriesName, now.minusHours(1002), now.minusHours(1001)));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, value(0, timeSeries));
        assertEquals(1001, value(1, timeSeries));
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesHourPoint(now.minusHours(20), 20);
        indexTimeSeriesHourPoint(now.minusHours(19), 19);
        indexTimeSeriesHourPoint(now.minusHours(18), 18);
        indexTimeSeriesHourPoint(now.minusHours(17), 17);
        List<TimeSeriesPoint> timeSeries = parseJson(hours(timeSeriesName, now.minusHours(9), now.minusHours(8)));
        assertEquals(0, size(timeSeries));
    }

    @Test @Ignore
    public void givenHourSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesHourPoint(now.minusHours(100), 100);
        indexTimeSeriesHourPoint(now.minusHours(200), 200);
        indexTimeSeriesHourPoint(now.minusHours(300), 300);
        indexTimeSeriesHourPoint(now.minusHours(400), 400);
        List<TimeSeriesPoint> timeSeries = parseJson(hours(timeSeriesName, now.minusYears(10), now.plusYears(10)));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesDayPoint(now.minusDays(1000), 1000);
        indexTimeSeriesDayPoint(now.minusDays(1001), 1001);
        indexTimeSeriesDayPoint(now.minusDays(1002), 1002);
        indexTimeSeriesDayPoint(now.minusDays(1003), 1003);
        List<TimeSeriesPoint> timeSeries = parseJson(days(timeSeriesName, now.minusDays(1002), now.minusDays(1001)));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, value(0, timeSeries));
        assertEquals(1001, value(1, timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesDayPoint(now.minusDays(20), 20);
        indexTimeSeriesDayPoint(now.minusDays(19), 19);
        indexTimeSeriesDayPoint(now.minusDays(18), 18);
        indexTimeSeriesDayPoint(now.minusDays(17), 17);
        List<TimeSeriesPoint> timeSeries = parseJson(days(timeSeriesName, now.minusDays(9), now.minusDays(8)));
        assertEquals(0, size(timeSeries));
    }

    @Test @Ignore
    public void givenDaySeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesDayPoint(now.minusDays(100), 100);
        indexTimeSeriesDayPoint(now.minusDays(200), 200);
        indexTimeSeriesDayPoint(now.minusDays(300), 300);
        indexTimeSeriesDayPoint(now.minusDays(400), 400);
        List<TimeSeriesPoint> timeSeries = parseJson(days(timeSeriesName, now.minusYears(10), now.plusYears(10)));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMonthPoint(now.minusMonths(1000), 1000);
        indexTimeSeriesMonthPoint(now.minusMonths(1001), 1001);
        indexTimeSeriesMonthPoint(now.minusMonths(1002), 1002);
        indexTimeSeriesMonthPoint(now.minusMonths(1003), 1003);
        List<TimeSeriesPoint> timeSeries = parseJson(months(timeSeriesName, now.minusMonths(1002), now.minusMonths(1001)));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, value(0, timeSeries));
        assertEquals(1001, value(1, timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMonthPoint(now.minusMonths(20), 20);
        indexTimeSeriesMonthPoint(now.minusMonths(19), 19);
        indexTimeSeriesMonthPoint(now.minusMonths(18), 18);
        indexTimeSeriesMonthPoint(now.minusMonths(17), 17);
        List<TimeSeriesPoint> timeSeries = parseJson(months(timeSeriesName, now.minusMonths(9), now.minusMonths(8)));
        assertEquals(0, size(timeSeries));
    }

    @Test @Ignore
    public void givenMonthSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMonthPoint(now.minusMonths(100), 100);
        indexTimeSeriesMonthPoint(now.minusMonths(200), 200);
        indexTimeSeriesMonthPoint(now.minusMonths(300), 300);
        indexTimeSeriesMonthPoint(now.minusMonths(400), 400);
        List<TimeSeriesPoint> timeSeries = parseJson(months(timeSeriesName, now.minusYears(10), now.plusYears(10)));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesYearPoint(now.minusYears(1), 1);
        indexTimeSeriesYearPoint(now.minusYears(2), 2);
        indexTimeSeriesYearPoint(now.minusYears(3), 3);
        indexTimeSeriesYearPoint(now.minusYears(4), 4);
        List<TimeSeriesPoint> timeSeries = parseJson(years(timeSeriesName, now.minusYears(3), now.minusYears(2)));
        assertEquals(2, size(timeSeries));
        assertEquals(3, value(0, timeSeries));
        assertEquals(2, value(1, timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesYearPoint(now.minusYears(20), 20);
        indexTimeSeriesYearPoint(now.minusYears(19), 19);
        indexTimeSeriesYearPoint(now.minusYears(18), 18);
        indexTimeSeriesYearPoint(now.minusYears(17), 17);
        List<TimeSeriesPoint> timeSeries = parseJson(years(timeSeriesName, now.minusYears(9), now.minusYears(8)));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesYearPoint(now.minusYears(1), 1);
        indexTimeSeriesYearPoint(now.minusYears(2), 2);
        indexTimeSeriesYearPoint(now.minusYears(3), 3);
        indexTimeSeriesYearPoint(now.minusYears(4), 4);
        List<TimeSeriesPoint> timeSeries = parseJson(years(timeSeriesName, now.minusYears(10), now.plusYears(10)));
        assertEquals(4, size(timeSeries));
    }

    private int value(int i, List<TimeSeriesPoint> timeSeries) throws IOException {
        return timeSeries.get(i).getMeasurement("value")
                .map(Measurement::getValue).orElseThrow(RuntimeException::new);
    }

    private int size(List<TimeSeriesPoint> timeSeries) throws IOException {
        return timeSeries.size();
    }

    private List<TimeSeriesPoint> parseJson(String json) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        return objectMapper.readValue(json, new TypeReference<List<TimeSeriesPoint>>(){});
    }

    private void indexTimeSeriesMinutePoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    private void indexTimeSeriesHourPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForHourSeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    private void indexTimeSeriesDayPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForDaySeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    private void indexTimeSeriesMonthPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForMonthSeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    private void indexTimeSeriesYearPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForYearSeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    private void indexTimeSeriesPoint(String indexName, String type, ZonedDateTime timestamp, int value) throws IOException {
        logger.info(format(
                "Executing indexing:\nIndex: %s\nType: %s\nTimestamp: %s\nValue: %d",
                indexName,
                type,
                timestamp,
                value
        ));
        client.prepareIndex(indexName, type)
                .setSource(
                        jsonBuilder().startObject()
                                .field("timestamp", formatTimestamp(timestamp))
                                .field("value", value)
                                .endObject()
                )
                .setRefresh(true) // Make document immediately searchable for the purpose of this test
                .get();
    }

    private String minutes(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return get(urlForSeries("minutes", seriesName, from, to)).asString();
    }

    private String hours(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return get(urlForSeries("hours", seriesName, from, to)).asString();
    }

    private String days(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return get(urlForSeries("days", seriesName, from, to)).asString();
    }

    private String months(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return get(urlForSeries("months", seriesName, from, to)).asString();
    }

    private String years(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return get(urlForSeries("years", seriesName, from, to)).asString();
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

    private String urlForSeries(String seriesTimeUnit, String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return format(
                "http://%s:%s/%s/%s/%s?from=%s&to=%s",
                System.getProperty("statistics.host"),
                System.getProperty("statistics.port"),
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

    private static Client elasticSearchClient() throws UnknownHostException {
        String host = System.getProperty("elasticsearch.host");
        int port = parseInt(System.getProperty("elasticsearch.port"));
        return TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    }


}
