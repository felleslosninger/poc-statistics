package no.difi.statistics.test.utils;


import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class ElasticsearchRule extends ElasticsearchContainer {

    /* This is set in ElasticsearchContainer as ELASTICSEARCH_DEFAULT_PORT=9200 */
    private final static int EXPOSED_CONTAINER_PORT = 9200;

    public ElasticsearchRule() {
        super("docker.elastic.co/elasticsearch/elasticsearch-oss:7.10.2");
    }

    public String getHost() {
        if (System.getenv("JENKINS_HOME") != null && System.getenv("DOCKER_HOST") == null) {
//            // Handle running in a Jenkins container without DOCKER_HOST specified. Host bound port can then be accessed
//            // through the container's gateway address.
            return this.getContainerInfo().getNetworkSettings().getGateway();
        } else {
            return super.getHost();
        }
    }

    public int getPort() {
        return super.getMappedPort(EXPOSED_CONTAINER_PORT);
    }

}
