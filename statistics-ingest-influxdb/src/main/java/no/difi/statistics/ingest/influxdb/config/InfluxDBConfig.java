package no.difi.statistics.ingest.influxdb.config;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.BackendConfig;
import no.difi.statistics.ingest.influxdb.InfluxDBIngestService;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import static java.lang.String.format;

public class InfluxDBConfig implements BackendConfig {

    @Autowired
    private Environment environment;

    @Override
    public IngestService ingestService() {
        return new InfluxDBIngestService(influxdbClient());
    }

    @Bean
    public InfluxDB influxdbClient() {
        String host = environment.getRequiredProperty("no.difi.statistics.influxdb.host");
        int port = environment.getRequiredProperty("no.difi.statistics.influxdb.port", Integer.class);
        return InfluxDBFactory.connect(format("http://%s:%d", host, port), "root", "root");
    }


}
