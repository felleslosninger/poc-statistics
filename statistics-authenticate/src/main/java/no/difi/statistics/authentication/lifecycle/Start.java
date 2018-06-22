package no.difi.statistics.authentication.lifecycle;

import no.difi.statistics.authentication.config.AppConfig;
import org.springframework.boot.SpringApplication;

public class Start {

    public static void main(String...args) {
        SpringApplication.run(AppConfig.class, args);
    }

}
