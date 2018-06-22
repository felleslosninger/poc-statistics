package no.difi.statistics.query.elasticsearch.lifecycle;

import no.difi.statistics.query.config.AppConfig;
import no.difi.statistics.query.elasticsearch.config.ElasticsearchConfig;
import org.springframework.boot.SpringApplication;

public class Start {

    public static void main(String...args) {
        SpringApplication.run(new Class[]{AppConfig.class, ElasticsearchConfig.class}, args);
    }

}
