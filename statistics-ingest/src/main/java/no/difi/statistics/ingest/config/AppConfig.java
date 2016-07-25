package no.difi.statistics.ingest.config;

import no.difi.statistics.ingest.api.IngestRestController;
import no.difi.statistics.ingest.poc.DifiAdminIngester;
import no.difi.statistics.ingest.poc.RandomIngester;
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
    public IngestRestController api() {
        return new IngestRestController(backendConfig.ingestService());
    }

    @Bean
    public RandomIngester randomIngester() {
        return new RandomIngester(backendConfig.ingestService());
    }

    @Bean
    public DifiAdminIngester difiAdminIngester() {
        return new DifiAdminIngester(backendConfig.ingestService());
    }

}
