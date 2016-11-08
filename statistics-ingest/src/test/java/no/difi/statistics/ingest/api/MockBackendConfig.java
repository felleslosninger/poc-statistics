package no.difi.statistics.ingest.api;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.BackendConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.userdetails.UserDetailsService;

import static org.mockito.Mockito.mock;

@Configuration
public class MockBackendConfig implements BackendConfig {

    @Override
    @Bean
    public IngestService ingestService() {
        return mock(IngestService.class);
    }

    @Override
    @Bean
    public UserDetailsService userDetailsService() {
        return mock(UserDetailsService.class);
    }

}
