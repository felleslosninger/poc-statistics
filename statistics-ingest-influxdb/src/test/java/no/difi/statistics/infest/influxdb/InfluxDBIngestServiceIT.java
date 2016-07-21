package no.difi.statistics.infest.influxdb;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.influxdb.config.AppConfig;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.test.utils.DockerHelper;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.SpringApplication;

import java.time.ZonedDateTime;

public class InfluxDBIngestServiceIT {

    private final static DockerHelper dockerHelper = new DockerHelper();
    private IngestService service;

    @Before
    public void init() {
        service = SpringApplication.run(
                AppConfig.class,
                "--no.difi.statistics.influxdb.host=" + dockerHelper.address(),
                "--no.difi.statistics.influxdb.port=" + dockerHelper.portFor(8086, "/influxdb")
        ).getBean(IngestService.class);
    }

    @Test
    public void doTest() {
        service.minute(
                "testSeries",
                "testDataType",
                TimeSeriesPoint.builder()
                        .timestamp(ZonedDateTime.now())
                        .measurement("testMeasurementId", 321)
                        .build());
    }

}
