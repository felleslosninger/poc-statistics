package no.difi.statistics.query.elasticsearch.lifecycle;

import no.difi.statistics.query.elasticsearch.config.AppConfig;
import org.springframework.boot.SpringApplication;

public class Start {

    public static void main(String...args) {
        SpringApplication.run(AppConfig.class, args);
    }

}
