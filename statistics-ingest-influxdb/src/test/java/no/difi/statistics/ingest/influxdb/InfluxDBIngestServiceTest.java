package no.difi.statistics.ingest.influxdb;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.ingest.influxdb.config.InfluxDBConfig;
import no.difi.statistics.model.TimeSeriesPoint;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(
        webEnvironment = RANDOM_PORT
)
@ContextConfiguration(classes = {AppConfig.class, InfluxDBConfig.class}, initializers = InfluxDBIngestServiceTest.Initializer.class)
@RunWith(SpringRunner.class)
public class InfluxDBIngestServiceTest {

    // BeforeClass + "BeforeApplicationContext"
    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            GenericContainer influxDB = new GenericContainer("influxdb:0.13.0");
            influxDB.start();
            EnvironmentTestUtils.addEnvironment(
                    applicationContext.getEnvironment(),
                    "no.difi.statistics.influxdb.host=" + influxDB.getContainerIpAddress(),
                    "no.difi.statistics.influxdb.port=" + influxDB.getMappedPort(8086)
            );
            InfluxDBIngestServiceTest.influxDB = influxDB;
        }

    }

    private static GenericContainer influxDB;

    @AfterClass
    public static void tearDown() {
        influxDB.stop();
    }

    @Autowired
    private IngestService service;
    @Autowired
    private InfluxDB influxClient;

    @Test
    public void givenADataPointWhenIngestingThenItCanBeFoundByQuery() {
        ZonedDateTime timestamp = ZonedDateTime.now();
        String seriesName = "testSeries";
        String measurementId = "testMeasurementId";
        long measurementValue = 321;
        service.minute(
                seriesName,
                TimeSeriesPoint.builder()
                        .timestamp(timestamp)
                        .measurement(measurementId, measurementValue)
                        .build());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        QueryResult result = influxClient.query(new Query("SELECT * from " + seriesName, "default"));
        assertDataPoint(seriesName, timestamp, measurementId, measurementValue, result);
    }

    private void assertDataPoint(String seriesName, ZonedDateTime timestamp, String measurementId, long measurementValue, QueryResult result) {
        assertFalse(result.hasError());
        assertEquals(1, result.getResults().size());
        assertNotNull(result.getResults().get(0).getSeries());
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
