package no.difi.statistics.ingest.client.demo.config;

import no.difi.statistics.ingest.client.IngestService;

import java.net.MalformedURLException;

public interface BackendConfig {

    IngestService ingestService() throws MalformedURLException;

}
