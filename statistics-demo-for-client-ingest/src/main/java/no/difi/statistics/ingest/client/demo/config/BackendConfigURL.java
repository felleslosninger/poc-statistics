package no.difi.statistics.ingest.client.demo.config;

import no.difi.statistics.ingest.client.IngestClient;
import no.difi.statistics.ingest.client.IngestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.net.URL;

@Configuration
public class BackendConfigURL implements BackendConfig {

    @Autowired
    private Environment environment;

    @Override
    @Bean
    public IngestService ingestService() throws IngestClient.MalformedUrl {
        return new IngestClient(
                environment.getRequiredProperty("baseUrl", URL.class),
                5000,
                15000,
                environment.getRequiredProperty("owner"),
                environment.getRequiredProperty("user"),
                environment.getRequiredProperty("pwd"));
    }
}
