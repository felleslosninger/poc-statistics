package no.difi.statistics.ingest.influxdb.lifecycle;

import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.ingest.influxdb.config.InfluxDBConfig;
import org.springframework.boot.SpringApplication;

import java.io.IOException;

public class Start {

    public static void main(String...args) throws IOException {
        SpringApplication.run(new Object[]{AppConfig.class, InfluxDBConfig.class}, args);
    }

}
