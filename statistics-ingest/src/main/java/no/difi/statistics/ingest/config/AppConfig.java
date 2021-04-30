package no.difi.statistics.ingest.config;

import no.difi.statistics.ingest.api.IngestRestController;
import no.difi.statistics.ingest.poc.RandomIngesterRestController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource("classpath:application.properties")
public class AppConfig  {


    @Autowired
    private BackendConfig backendConfig;

    @Bean
    public IngestRestController api() {
        return new IngestRestController(backendConfig.ingestService());
    }

    @Bean
    public RandomIngesterRestController randomApi() {
        return new RandomIngesterRestController(backendConfig.ingestService());
    }

}
