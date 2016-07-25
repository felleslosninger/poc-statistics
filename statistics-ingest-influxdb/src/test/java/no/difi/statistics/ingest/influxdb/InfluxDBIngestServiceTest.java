package no.difi.statistics.ingest.influxdb;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.TimeSeriesPoint;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static java.lang.String.format;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;

public class InfluxDBIngestServiceTest {

    @ClassRule
    public static GenericContainer influxDB = new GenericContainer("influxdb:0.13.0");

    private IngestService service;
    private InfluxDB influxClient;

    @Before
    public void init() {
        influxClient = InfluxDBFactory.connect(format(
                "http://%s:%d",
                influxDB.getContainerIpAddress(),
                influxDB.getMappedPort(8086)
        ), "root", "root");
        service = new InfluxDBIngestService(influxClient);
    }

    @Test
    public void givenADataPointWhenIngestingThenItCanBeFoundByQuery() {
        ZonedDateTime timestamp = ZonedDateTime.now();
        String seriesName = "testSeries";
        String measurementId = "testMeasurementId";
        int measurementValue = 321;
        service.minute(
                seriesName,
                TimeSeriesPoint.builder()
                        .timestamp(timestamp)
                        .measurement(measurementId, measurementValue)
                        .build());
        QueryResult result = influxClient.query(new Query("SELECT * from " + seriesName, "default"));
        assertDataPoint(seriesName, timestamp, measurementId, measurementValue, result);
    }

    private void assertDataPoint(String seriesName, ZonedDateTime timestamp, String measurementId, int measurementValue, QueryResult result) {
        assertFalse(result.hasError());
        assertEquals(1, result.getResults().size());
        assertEquals(1, result.getResults().get(0).getSeries().size());
        QueryResult.Series series = result.getResults().get(0).getSeries().get(0);
        assertEquals(seriesName, series.getName());
        assertEquals(2, series.getColumns().size());
        assertEquals("time", series.getColumns().get(0));
        assertEquals(formatTimestamp(timestamp), series.getValues().get(0).get(0));
        assertEquals(measurementId, series.getColumns().get(1));
        assertEquals(measurementValue, ((Double)series.getValues().get(0).get(1)).intValue());
    }

    private static String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp.withZoneSameInstant(ZoneOffset.UTC));
    }

}
