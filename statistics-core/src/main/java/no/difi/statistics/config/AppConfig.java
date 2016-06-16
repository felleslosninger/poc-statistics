package no.difi.statistics.config;

import no.difi.statistics.Statistics;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class AppConfig {

    @Autowired
    private Environment environment;

    @Bean(destroyMethod = "close")
    public Client elasticSearchClient() throws UnknownHostException {
        String host = environment.getRequiredProperty("elasticsearch.host");
        int port = environment.getRequiredProperty("elasticsearch.port", Integer.class);
        return TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    }

    @Bean
    public Statistics statistics() throws UnknownHostException {
        return new Statistics(elasticSearchClient());
    }

}
