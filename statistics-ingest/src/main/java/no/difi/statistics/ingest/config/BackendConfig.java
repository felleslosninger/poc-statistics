package no.difi.statistics.ingest.config;

import no.difi.statistics.ingest.IngestService;
import org.springframework.security.authentication.AuthenticationProvider;

public interface BackendConfig {

    IngestService ingestService();

    AuthenticationProvider authenticationProvider();

}
