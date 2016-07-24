package no.difi.statistics.query.config;

import no.difi.statistics.query.api.QueryRestController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration
public class AppConfig {

    @Autowired
    private BackendConfig backendConfig;

    @Bean
    public QueryRestController api() {
        return new QueryRestController(backendConfig.queryService());
    }

}
