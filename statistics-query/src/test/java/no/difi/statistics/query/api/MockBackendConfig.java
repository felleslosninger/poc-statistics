package no.difi.statistics.query.api;

import no.difi.statistics.query.QueryService;
import no.difi.statistics.query.config.BackendConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.mockito.Mockito.mock;

@Configuration
public class MockBackendConfig implements BackendConfig {

    @Bean
    public QueryService queryService() {
        return mock(QueryService.class);
    }

}
