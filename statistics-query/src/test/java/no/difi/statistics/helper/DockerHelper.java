package no.difi.statistics.helper;

import io.fabric8.docker.api.model.Container;
import io.fabric8.docker.api.model.Port;
import io.fabric8.docker.client.Config;
import io.fabric8.docker.client.ConfigBuilder;
import io.fabric8.docker.client.DefaultDockerClient;
import io.fabric8.docker.client.DockerClient;

import java.net.URI;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public class DockerHelper {

    private DockerClient client;
    private String address;

    public DockerHelper() {
        Config config = new ConfigBuilder().build();
        if (config.getDockerUrl() == null) throw new IllegalStateException("Environment variable DOCKER_HOST not set");
        address = URI.create(config.getDockerUrl().replace("///", "//")).getHost();
        if (address == null) throw new IllegalArgumentException("Environment variable DOCKER_HOST has invalid value: " + config.getDockerUrl());
        client = new DefaultDockerClient(config);
    }

    public String address() {
        return address;
    }

    public int portFor(int privatePort, String containerName) {
        return container(containerName, client)
                .getPorts().stream()
                .filter(p -> p.getPrivatePort() == privatePort)
                .map(Port::getPublicPort)
                .findFirst()
                .orElseThrow(() -> new RuntimeException(format(
                                "Unable to find public port for container %s's private port %d",
                                containerName,
                                privatePort
                        )));
    }

    private Container container(String name, DockerClient client) {
        return client.container().list().running().stream()
                .filter(c -> {
                        System.out.println("Container id: " + c.getId());
                        System.out.println("Container names: " + c.getNames());
                        return c.getNames().contains(name);
                    }
                ).findFirst()
                .orElseThrow(() -> new RuntimeException(format("No container with name %s found", name)));
    }


}
