package no.difi.statistics.query.elasticsearch;

import no.difi.statistics.elasticsearch.IndexNameResolver;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;

import static java.util.stream.Collectors.toList;
import static no.difi.statistics.model.MeasurementDistance.*;

public class GetAvailableTimeSeries {

    private RestClient elasticSearchClient;

    private GetAvailableTimeSeries() {
        // Use builder
    }

    private List<TimeSeriesDefinition> doExecute() {
        List<String> indices = new ArrayList<>();
        try (InputStream response = elasticSearchClient.performRequest("GET", "/_cat/indices?h=index").getEntity().getContent();
             Scanner scanner = new Scanner(response)) {
            scanner.forEachRemaining(indices::add);
        } catch (IOException e) {
            throw new RuntimeException("Failed to list available time series", e);
        }
        return indices.stream()
                .map(IndexNameResolver.pattern()::matcher)
                .filter(Matcher::find)
                .map(matcher -> TimeSeriesDefinition.builder()
                        .name(matcher.group(2))
                        .distance(distanceFrom(matcher.group(3)))
                        .owner(matcher.group(1))
                )
                .distinct()
                .sorted()
                .collect(toList());
    }

    private MeasurementDistance distanceFrom(String indexNamePatternGroup3) {
        switch (indexNamePatternGroup3) {
            case "minute": return minutes;
            case "hour": return hours;
            case "day": return days;
            case "month": return months;
            case "year": return years;
            default: throw new IllegalArgumentException(indexNamePatternGroup3);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private GetAvailableTimeSeries instance = new GetAvailableTimeSeries();

        public Builder elasticsearchClient(RestClient client) {
            instance.elasticSearchClient = client;
            return this;
        }

        List<TimeSeriesDefinition> execute() {
            return instance.doExecute();
        }

    }

}
