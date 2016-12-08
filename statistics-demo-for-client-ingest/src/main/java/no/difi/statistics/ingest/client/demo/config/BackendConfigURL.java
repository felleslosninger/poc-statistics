package no.difi.statistics.ingest.client.demo.config;

import no.difi.statistics.ingest.client.IngestClient;
import no.difi.statistics.ingest.client.IngestService;
import no.difi.statistics.ingest.client.exception.MalformedUrl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class BackendConfigURL implements BackendConfig {

    @Autowired
    private Environment environment;

    @Override
    @Bean
    public IngestService ingestService() throws MalformedUrl {
        return new IngestClient(
                environment.getRequiredProperty("service.url"),
                5000,
                15000,
                environment.getRequiredProperty("owner"),
                environment.getRequiredProperty("user"),
                environment.getRequiredProperty("pwd"));
    }
}
