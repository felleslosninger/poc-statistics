package no.difi.statistics.ingest.client.demo;

import no.difi.statistics.ingest.client.IngestService;
import no.difi.statistics.ingest.client.demo.config.BackendConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.mockito.Mockito.mock;

@Configuration
public class MockBackendConfig implements BackendConfig {

    @Override
    @Bean
    public IngestService ingestService() {
        return mock(IngestService.class);
    }
}
