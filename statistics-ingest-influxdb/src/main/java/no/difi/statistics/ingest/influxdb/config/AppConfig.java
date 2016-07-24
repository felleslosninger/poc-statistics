package no.difi.statistics.ingest.influxdb.config;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.api.IngestRestController;
import no.difi.statistics.ingest.influxdb.InfluxDBIngestService;
import no.difi.statistics.ingest.poc.DifiAdminIngester;
import no.difi.statistics.ingest.poc.RandomIngester;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.net.UnknownHostException;

import static java.lang.String.format;

@Configuration
@EnableAutoConfiguration
public class AppConfig {

    @Autowired
    private Environment environment;

    @Bean
    public IngestRestController api() {
        return new IngestRestController(ingestService());
    }

    @Bean
    public IngestService ingestService(){
        return new InfluxDBIngestService(influxdbClient());
    }

    @Bean
    public InfluxDB influxdbClient() {
        String host = environment.getRequiredProperty("no.difi.statistics.influxdb.host");
        int port = environment.getRequiredProperty("no.difi.statistics.influxdb.port", Integer.class);
        return InfluxDBFactory.connect(format("http://%s:%d", host, port), "root", "root");
    }

    @Bean
    public RandomIngester randomIngester() throws IOException {
        return new RandomIngester(ingestService());
    }

    @Bean
    public DifiAdminIngester difiAdminIngester() throws UnknownHostException {
        return new DifiAdminIngester(ingestService());
    }

}
