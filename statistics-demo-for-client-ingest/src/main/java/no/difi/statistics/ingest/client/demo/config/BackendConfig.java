package no.difi.statistics.ingest.client.demo.config;

import no.difi.statistics.ingest.client.IngestService;
import no.difi.statistics.ingest.client.exception.MailformedUrl;

public interface BackendConfig {

    IngestService ingestService() throws MailformedUrl;

}
