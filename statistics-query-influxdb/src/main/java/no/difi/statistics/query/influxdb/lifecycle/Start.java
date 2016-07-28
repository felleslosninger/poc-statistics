package no.difi.statistics.query.influxdb.lifecycle;

import no.difi.statistics.query.config.AppConfig;
import no.difi.statistics.query.influxdb.config.InfluxDBConfig;
import org.springframework.boot.SpringApplication;

public class Start {

    public static void main(String...args) {
        SpringApplication.run(new Object[]{
                AppConfig.class,
                InfluxDBConfig.class
        }, args);
    }

}
