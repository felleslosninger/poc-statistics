package no.difi.statistics.ingest.client.demo.config;

import no.difi.statistics.ingest.client.IngestClient;
import no.difi.statistics.ingest.client.IngestService;

public interface BackendConfig {

    IngestService ingestService() throws IngestClient.MalformedUrl;

}
