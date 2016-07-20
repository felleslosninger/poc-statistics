package no.difi.statistics.ingest.influxdb.lifecycle;

import no.difi.statistics.ingest.influxdb.config.AppConfig;
import org.springframework.boot.SpringApplication;

import java.io.IOException;

public class Start {

    public static void main(String...args) throws IOException {
        SpringApplication.run(AppConfig.class, args);
    }

}
