package no.difi.statistics.lifecycle;

import no.difi.statistics.config.AppConfig;
import org.springframework.boot.SpringApplication;

public class Start {

    public static void main(String...args) {
        SpringApplication.run(AppConfig.class, args);
    }

}
