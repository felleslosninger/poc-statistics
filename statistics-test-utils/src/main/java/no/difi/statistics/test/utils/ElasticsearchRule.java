package no.difi.statistics.test.utils;

import com.arakelian.docker.junit.DockerRule;
import com.arakelian.docker.junit.model.ImmutableDockerConfig;
import com.spotify.docker.client.messages.PortBinding;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ElasticsearchRule extends DockerRule {

    public ElasticsearchRule() {
        super(ImmutableDockerConfig.builder()
                .name(UUID.randomUUID().toString())
                .image("docker.elastic.co/elasticsearch/elasticsearch-oss:6.3.0")
                .ports("9200/tcp")
                .alwaysRemoveContainer(true)
                .allowRunningBetweenUnitTests(true)
                .addContainerConfigurer(c -> c.env("network.host=_site_"))
                .build());
    }

    public String getHost() {
        if (System.getenv("JENKINS_HOME") != null && System.getenv("DOCKER_HOST") == null) {
            // Handle running in a Jenkins container without DOCKER_HOST specified. Host bound port can then be accessed
            // through the container's gateway address.
            return getContainer().getInfo().networkSettings().gateway();
        } else {
            return getContainer().getClient().getHost();
        }
    }

    public int getPort() {
        final String portName = "9200/tcp";
        final Map<String, List<PortBinding>> ports = getContainer().getInfo().networkSettings().ports();
        final List<PortBinding> portBindings = ports.get(portName);
        if (portBindings != null && !portBindings.isEmpty()) {
            final PortBinding firstBinding = portBindings.get(0);
            return Integer.parseInt(firstBinding.hostPort());
        }
        return -1;
    }

}
