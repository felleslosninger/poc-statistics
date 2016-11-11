package no.difi.statistics.authentication.lifecycle;

import no.difi.statistics.authentication.config.AppConfig;
import org.springframework.boot.SpringApplication;

import java.io.IOException;

public class Start {

    public static void main(String...args) throws IOException {
        SpringApplication.run(AppConfig.class, args);
    }

}
