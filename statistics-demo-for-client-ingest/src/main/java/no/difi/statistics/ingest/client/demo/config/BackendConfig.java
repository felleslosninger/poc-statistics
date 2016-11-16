package no.difi.statistics.ingest.client.demo.config;

import no.difi.statistics.ingest.client.IngestService;
import no.difi.statistics.ingest.client.exception.MalformedUrl;

public interface BackendConfig {

    IngestService ingestService() throws MalformedUrl;

}
