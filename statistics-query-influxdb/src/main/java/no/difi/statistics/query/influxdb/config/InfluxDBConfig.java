package no.difi.statistics.query.influxdb.config;

import no.difi.statistics.query.QueryService;
import no.difi.statistics.query.config.BackendConfig;
import no.difi.statistics.query.influxdb.InfluxDBQueryService;
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
    @Bean
    public QueryService queryService() {
        return new InfluxDBQueryService(influxDBClient());
    }

    @Bean
    public InfluxDB influxDBClient() {
        String host = environment.getRequiredProperty("no.difi.statistics.influxdb.host");
        int port = environment.getRequiredProperty("no.difi.statistics.influxdb.port", Integer.class);
        return InfluxDBFactory.connect(format(
                "http://%s:%d",
                host,
                port
        ), "root", "root");
    }

}
