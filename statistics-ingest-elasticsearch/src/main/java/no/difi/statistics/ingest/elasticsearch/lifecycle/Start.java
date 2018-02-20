package no.difi.statistics.ingest.elasticsearch.lifecycle;

import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.ingest.elasticsearch.config.ElasticsearchConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Start {

    public static void main(String...args) {
        SpringApplication.run(new Object[]{AppConfig.class, ElasticsearchConfig.class}, args);
    }

}
