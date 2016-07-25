package no.difi.statistics.query.config;

import no.difi.statistics.query.QueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

public interface BackendConfig {

    QueryService queryService();

}
