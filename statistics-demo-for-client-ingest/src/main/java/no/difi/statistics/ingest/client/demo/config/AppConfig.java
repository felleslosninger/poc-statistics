package no.difi.statistics.ingest.client.demo.config;

import no.difi.statistics.ingest.client.IngestClient;
import no.difi.statistics.ingest.client.demo.api.IngestRestController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration
public class AppConfig {

    @Autowired
    private BackendConfig backendConfig;

    @Bean
    public IngestRestController api() throws IngestClient.MalformedUrl {
        return new IngestRestController(backendConfig.ingestService());
    }
}