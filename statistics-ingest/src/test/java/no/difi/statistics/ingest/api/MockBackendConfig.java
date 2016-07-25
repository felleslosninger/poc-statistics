package no.difi.statistics.ingest.api;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.BackendConfig;

import static org.mockito.Mockito.mock;

public class MockBackendConfig implements BackendConfig {

    @Override
    public IngestService ingestService() {
        return mock(IngestService.class);
    }

}
