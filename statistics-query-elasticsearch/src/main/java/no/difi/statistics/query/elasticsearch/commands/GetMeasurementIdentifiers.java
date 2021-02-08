package no.difi.statistics.query.elasticsearch.commands;

import no.difi.statistics.elasticsearch.IndexNameResolver;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import javax.json.Json;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class GetMeasurementIdentifiers {

    private static final String timeFieldName = "timestamp";
    private RestClient elasticsearchClient;
    private List<String> indexNames;

    private List<String> doExecute() {
        String genericIndexName = IndexNameResolver.generic(indexNames.get(0));
        Set<String> result = new HashSet<>();
        Request request = new Request("GET", "/" + genericIndexName + "/_mappings?ignore_unavailable=true");
        try (InputStream response = elasticsearchClient
                .performRequest(request)
                .getEntity().getContent()) {
            JsonReader reader = Json.createReader(response);
            reader.readObject().forEach(
                    (key, value) -> result.addAll(
                            value.asJsonObject().getJsonObject("mappings")
                                    .getJsonObject("properties").keySet().stream()
                                    .filter(p -> !p.startsWith("category."))
                                    .filter(p -> !p.equals("category"))
                                    .filter(p -> !p.equals(timeFieldName))
                                    .collect(toSet())
                    )
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to get available measurement ids", e);
        }
        return new ArrayList<>(result);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private GetMeasurementIdentifiers instance = new GetMeasurementIdentifiers();

        public Builder elasticsearchClient(RestClient client) {
            instance.elasticsearchClient = client;
            return this;
        }

        Builder indexNames(List<String> indexNames) {
            instance.indexNames = indexNames;
            return this;
        }

        List<String> execute() {
            return instance.doExecute();
        }

    }
}
