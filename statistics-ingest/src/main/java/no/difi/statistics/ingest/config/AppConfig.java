package no.difi.statistics.ingest.config;

import no.difi.statistics.ingest.DifiAdminIngester;
import no.difi.statistics.ingest.RandomIngester;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
@EnableAutoConfiguration
public class AppConfig {

    @Autowired
    private Environment environment;

    @Bean(destroyMethod = "close")
    public Client elasticSearchClient() throws UnknownHostException {
        String host = environment.getRequiredProperty("no.difi.statistics.elasticsearch.host");
        int port = environment.getRequiredProperty("no.difi.statistics.elasticsearch.port", Integer.class);
        return TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    }

    @Bean
    public RandomIngester randomIngester() throws IOException {
        return new RandomIngester(elasticSearchClient());
    }

    @Bean
    public DifiAdminIngester difiAdminIngester() throws UnknownHostException {
        return new DifiAdminIngester(elasticSearchClient());
    }

}
