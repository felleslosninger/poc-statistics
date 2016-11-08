package no.difi.statistics.ingest.config;

import no.difi.statistics.ingest.IngestService;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.userdetails.UserDetailsService;

public interface BackendConfig {

    IngestService ingestService();

    UserDetailsService userDetailsService();

}
