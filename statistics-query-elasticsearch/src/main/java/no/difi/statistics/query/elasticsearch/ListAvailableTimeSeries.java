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

public class ListAvailableTimeSeries {

    private RestClient elasticSearchClient;

    private ListAvailableTimeSeries() {
        // Use builder
    }

    List<TimeSeriesDefinition> execute() {
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

    public static Command builder() {
        return new Command();
    }

    public static class Command {

        private RestClient elasticSearchClient;

        public Command elasticsearchClient(RestClient client) {
            this.elasticSearchClient = client;
            return this;
        }

        List<TimeSeriesDefinition> execute() {
            ListAvailableTimeSeries command = new ListAvailableTimeSeries();
            command.elasticSearchClient = elasticSearchClient;
            return command.execute();
        }

    }




}
