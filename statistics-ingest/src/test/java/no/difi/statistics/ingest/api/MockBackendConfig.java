package no.difi.statistics.ingest.api;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.BackendConfig;
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
