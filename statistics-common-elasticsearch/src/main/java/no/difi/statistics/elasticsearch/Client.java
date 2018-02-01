package no.difi.statistics.elasticsearch;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Client {

    private RestHighLevelClient highLevelClient;
    private String address;

    public Client(RestHighLevelClient highLevelClient, String address) {
        this.highLevelClient = highLevelClient;
        this.address = address;
    }

    public RestHighLevelClient highLevel() {
        return highLevelClient;
    }

    public RestClient lowLevel() {
        return highLevelClient.getLowLevelClient();
    }

    public String toString() {
        return address;
    }

}
