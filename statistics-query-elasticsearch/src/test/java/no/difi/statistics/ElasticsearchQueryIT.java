package no.difi.statistics;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Ignore
public class ElasticsearchQueryIT extends AbstractQueryIT {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static Client elasticsearchClient;
    private final static String timeSeriesName = "test";
    private final static String measurementId = "count";

    @BeforeClass
    public static void initAll() throws UnknownHostException {
        elasticsearchClient = elasticSearchClient(
                AbstractQueryIT.dockerHelper.address(),
                AbstractQueryIT.dockerHelper.portFor(9300, "/elasticsearch")
        );
    }

    @Override
    protected String apiContainerName() {
        return "/statistics-query-elasticsearch";
    }

    @After
    public void cleanup() throws ExecutionException, InterruptedException {
        elasticsearchClient.admin().indices().prepareDelete("_all").get();
    }

    @Override
    protected void indexMinutePointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, timestamp), "total", timestamp, value);
            timestamp = timestamp.plusMinutes(1);
        }
    }

    @Override
    protected void indexHourPointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForHourSeries(timeSeriesName, timestamp), "total", timestamp, value);
            timestamp = timestamp.plusHours(1);
        }
    }

    @Override
    protected void indexDayPointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForDaySeries(timeSeriesName, timestamp), "total", timestamp, value);
            timestamp = timestamp.plusDays(1);
        }
    }

    @Override
    protected void indexMonthPointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForMonthSeries(timeSeriesName, timestamp), "total", timestamp, value);
            timestamp = timestamp.plusMonths(1);
        }
    }

    @Override
    protected void indexYearPointsFrom(ZonedDateTime timestamp, int...values) throws IOException {
        for (int value : values) {
            indexTimeSeriesPoint(indexNameForYearSeries(timeSeriesName, timestamp), "total", timestamp, value);
            timestamp = timestamp.plusYears(1);
        }
    }

    @Override
    protected void indexMinutePoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    @Override
    protected void indexHourPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForHourSeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    @Override
    protected void indexDayPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForDaySeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    @Override
    protected void indexMonthPoint(ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexNameForMonthSeries(timeSeriesName, timestamp), "total", timestamp, value);
    }

    private void indexTimeSeriesPoint(String indexName, String type, TimeSeriesPoint point) throws IOException {
        logger.info(format(
                "Executing indexing:\nIndex: %s\nType: %s\nPoint: %s",
                indexName,
                type,
                point
        ));
        XContentBuilder sourceBuilder = jsonBuilder().startObject()
                .field("timestamp", formatTimestamp(point.getTimestamp()));
        for (Measurement measurement : point.getMeasurements())
            sourceBuilder.field(measurement.getId(), measurement.getValue());
        elasticsearchClient.prepareIndex(indexName, type)
                .setSource(sourceBuilder.endObject())
                .setRefresh(true) // Make document immediately searchable for the purpose of this test
                .get();
    }

    @Override
    protected void indexMinutePoints(List<TimeSeriesPoint> minutePoints) throws IOException {
        for (TimeSeriesPoint point : minutePoints)
            indexTimeSeriesPoint(indexNameForMinuteSeries(timeSeriesName, point.getTimestamp()), "total", point);
    }

    private void indexTimeSeriesPoint(String indexName, String type, ZonedDateTime timestamp, int value) throws IOException {
        indexTimeSeriesPoint(indexName, type, TimeSeriesPoint.builder().timestamp(timestamp).measurement(measurementId, value).build());
    }

    private static Client elasticSearchClient(String host, int port) throws UnknownHostException {
        return TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
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

}
