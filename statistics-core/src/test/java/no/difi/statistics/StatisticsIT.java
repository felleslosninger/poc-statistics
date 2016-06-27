package no.difi.statistics;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.restassured.RestAssured.get;
import static io.restassured.path.json.JsonPath.from;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

public class StatisticsIT {

    private String timeSeriesName = "test";
    private ZonedDateTime now = ZonedDateTime.now();
    private static Client client;

    @BeforeClass
    public static void initAll() throws UnknownHostException {
        client = elasticSearchClient();
    }

    @After
    public void cleanup() throws ExecutionException, InterruptedException {
        client.admin().indices().delete(new DeleteIndexRequest(timeSeriesName)).get();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMinutePoint(now.minusMinutes(1000), 1000);
        indexTimeSeriesMinutePoint(now.minusMinutes(1001), 1001);
        indexTimeSeriesMinutePoint(now.minusMinutes(1002), 1002);
        indexTimeSeriesMinutePoint(now.minusMinutes(1003), 1003);
        List<HashMap<String, ?>> timeSeries = from(minutes(timeSeriesName, now.minusMinutes(1002), now.minusMinutes(1001))).getList("");
        assertEquals(1002, timeSeries.get(0).get("value"));
        assertEquals(1001, timeSeries.get(1).get("value"));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMinutePoint(now.minusMinutes(20), 20);
        indexTimeSeriesMinutePoint(now.minusMinutes(19), 19);
        indexTimeSeriesMinutePoint(now.minusMinutes(18), 18);
        indexTimeSeriesMinutePoint(now.minusMinutes(17), 17);
        List<HashMap<String, ?>> timeSeries = from(minutes(timeSeriesName, now.minusMinutes(9), now.minusMinutes(8))).getList("");
        assertEquals(0, timeSeries.size());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMinutePoint(now.minusMinutes(100), 100);
        indexTimeSeriesMinutePoint(now.minusMinutes(200), 200);
        indexTimeSeriesMinutePoint(now.minusMinutes(300), 300);
        indexTimeSeriesMinutePoint(now.minusMinutes(400), 400);
        List<HashMap<String, ?>> timeSeries = from(minutes(timeSeriesName, now.minusYears(2000), now.plusYears(2000))).getList("");
        assertEquals(4, timeSeries.size());
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesHourPoint(now.minusHours(1000), 1000);
        indexTimeSeriesHourPoint(now.minusHours(1001), 1001);
        indexTimeSeriesHourPoint(now.minusHours(1002), 1002);
        indexTimeSeriesHourPoint(now.minusHours(1003), 1003);
        List<HashMap<String, ?>> timeSeries = from(hours(timeSeriesName, now.minusHours(1002), now.minusHours(1001))).getList("");
        assertEquals(2, timeSeries.size());
        assertEquals(1002, timeSeries.get(0).get("value"));
        assertEquals(1001, timeSeries.get(1).get("value"));
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesHourPoint(now.minusHours(20), 20);
        indexTimeSeriesHourPoint(now.minusHours(19), 19);
        indexTimeSeriesHourPoint(now.minusHours(18), 18);
        indexTimeSeriesHourPoint(now.minusHours(17), 17);
        List<HashMap<String, ?>> timeSeries = from(hours(timeSeriesName, now.minusHours(9), now.minusHours(8))).getList("");
        assertEquals(0, timeSeries.size());
    }

    @Test
    public void givenHourSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesHourPoint(now.minusHours(100), 100);
        indexTimeSeriesHourPoint(now.minusHours(200), 200);
        indexTimeSeriesHourPoint(now.minusHours(300), 300);
        indexTimeSeriesHourPoint(now.minusHours(400), 400);
        List<HashMap<String, ?>> timeSeries = from(hours(timeSeriesName, now.minusYears(2000), now.plusYears(2000))).getList("");
        assertEquals(4, timeSeries.size());
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesDayPoint(now.minusDays(1000), 1000);
        indexTimeSeriesDayPoint(now.minusDays(1001), 1001);
        indexTimeSeriesDayPoint(now.minusDays(1002), 1002);
        indexTimeSeriesDayPoint(now.minusDays(1003), 1003);
        List<HashMap<String, ?>> timeSeries = from(days(timeSeriesName, now.minusDays(1002), now.minusDays(1001))).getList("");
        assertEquals(2, timeSeries.size());
        assertEquals(1002, timeSeries.get(0).get("value"));
        assertEquals(1001, timeSeries.get(1).get("value"));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesDayPoint(now.minusDays(20), 20);
        indexTimeSeriesDayPoint(now.minusDays(19), 19);
        indexTimeSeriesDayPoint(now.minusDays(18), 18);
        indexTimeSeriesDayPoint(now.minusDays(17), 17);
        List<HashMap<String, ?>> timeSeries = from(days(timeSeriesName, now.minusDays(9), now.minusDays(8))).getList("");
        assertEquals(0, timeSeries.size());
    }

    @Test
    public void givenDaySeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesDayPoint(now.minusDays(100), 100);
        indexTimeSeriesDayPoint(now.minusDays(200), 200);
        indexTimeSeriesDayPoint(now.minusDays(300), 300);
        indexTimeSeriesDayPoint(now.minusDays(400), 400);
        List<HashMap<String, ?>> timeSeries = from(days(timeSeriesName, now.minusYears(2000), now.plusYears(2000))).getList("");
        assertEquals(4, timeSeries.size());
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMonthPoint(now.minusMonths(1000), 1000);
        indexTimeSeriesMonthPoint(now.minusMonths(1001), 1001);
        indexTimeSeriesMonthPoint(now.minusMonths(1002), 1002);
        indexTimeSeriesMonthPoint(now.minusMonths(1003), 1003);
        List<HashMap<String, ?>> timeSeries = from(months(timeSeriesName, now.minusMonths(1002), now.minusMonths(1001))).getList("");
        assertEquals(2, timeSeries.size());
        assertEquals(1002, timeSeries.get(0).get("value"));
        assertEquals(1001, timeSeries.get(1).get("value"));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMonthPoint(now.minusMonths(20), 20);
        indexTimeSeriesMonthPoint(now.minusMonths(19), 19);
        indexTimeSeriesMonthPoint(now.minusMonths(18), 18);
        indexTimeSeriesMonthPoint(now.minusMonths(17), 17);
        List<HashMap<String, ?>> timeSeries = from(months(timeSeriesName, now.minusMonths(9), now.minusMonths(8))).getList("");
        assertEquals(0, timeSeries.size());
    }

    @Test
    public void givenMonthSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMonthPoint(now.minusMonths(100), 100);
        indexTimeSeriesMonthPoint(now.minusMonths(200), 200);
        indexTimeSeriesMonthPoint(now.minusMonths(300), 300);
        indexTimeSeriesMonthPoint(now.minusMonths(400), 400);
        List<HashMap<String, ?>> timeSeries = from(months(timeSeriesName, now.minusYears(2000), now.plusYears(2000))).getList("");
        assertEquals(4, timeSeries.size());
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesYearPoint(now.minusYears(1000), 1000);
        indexTimeSeriesYearPoint(now.minusYears(1001), 1001);
        indexTimeSeriesYearPoint(now.minusYears(1002), 1002);
        indexTimeSeriesYearPoint(now.minusYears(1003), 1003);
        List<HashMap<String, ?>> timeSeries = from(years(timeSeriesName, now.minusYears(1002), now.minusYears(1001))).getList("");
        assertEquals(2, timeSeries.size());
        assertEquals(1002, timeSeries.get(0).get("value"));
        assertEquals(1001, timeSeries.get(1).get("value"));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesYearPoint(now.minusYears(20), 20);
        indexTimeSeriesYearPoint(now.minusYears(19), 19);
        indexTimeSeriesYearPoint(now.minusYears(18), 18);
        indexTimeSeriesYearPoint(now.minusYears(17), 17);
        List<HashMap<String, ?>> timeSeries = from(years(timeSeriesName, now.minusYears(9), now.minusYears(8))).getList("");
        assertEquals(0, timeSeries.size());
    }

    @Test
    public void givenYearSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesYearPoint(now.minusYears(100), 100);
        indexTimeSeriesYearPoint(now.minusYears(200), 200);
        indexTimeSeriesYearPoint(now.minusYears(300), 300);
        indexTimeSeriesYearPoint(now.minusYears(400), 400);
        List<HashMap<String, ?>> timeSeries = from(years(timeSeriesName, now.minusYears(2000), now.plusYears(2000))).getList("");
        assertEquals(4, timeSeries.size());
    }

    private void indexTimeSeriesMinutePoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint("minutes", timestamp, value);
    }

    private void indexTimeSeriesHourPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint("hours", timestamp, value);
    }

    private void indexTimeSeriesDayPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint("days", timestamp, value);
    }

    private void indexTimeSeriesMonthPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint("months", timestamp, value);
    }

    private void indexTimeSeriesYearPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint("years", timestamp, value);
    }

    private void indexTimeSeriesPoint(String type, ZonedDateTime timestamp, int value) throws IOException {
        client.prepareIndex(timeSeriesName, type)
                .setSource(
                        jsonBuilder().startObject()
                                .field("time", formatTimestamp(timestamp))
                                .field("value", value)
                                .endObject()
                )
                .setRefresh(true) // Make document immediately searchable for the purpose of this test
                .get();
    }

    private String minutes(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return get(urlForMinuteSeries(seriesName, from, to)).asString();
    }

    private String hours(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return get(urlForHourSeries(seriesName, from, to)).asString();
    }

    private String days(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return get(urlForDaySeries(seriesName, from, to)).asString();
    }

    private String months(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return get(urlForMonthSeries(seriesName, from, to)).asString();
    }

    private String years(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return get(urlForYearSeries(seriesName, from, to)).asString();
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

    private String urlForMinuteSeries(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return urlForSeries("minutes", seriesName, from, to);
    }

    private String urlForHourSeries(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return urlForSeries("hours", seriesName, from, to);
    }

    private String urlForDaySeries(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return urlForSeries("days", seriesName, from, to);
    }

    private String urlForMonthSeries(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return urlForSeries("months", seriesName, from, to);
    }

    private String urlForYearSeries(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        return urlForSeries("years", seriesName, from, to);
    }

    private String urlForSeries(String type, String seriesName, ZonedDateTime from, ZonedDateTime to) {
        String host = System.getProperty("statistics.host");
        String port = System.getProperty("statistics.port");
        return format(
                "http://%s:%s/%s/%s?from=%s&to=%s",
                host,
                port,
                type,
                seriesName,
                formatTimestamp(from),
                formatTimestamp(to)
        );
    }

    private static Client elasticSearchClient() throws UnknownHostException {
        String host = System.getProperty("elasticsearch.host");
        int port = parseInt(System.getProperty("elasticsearch.port"));
        return TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    }


}
