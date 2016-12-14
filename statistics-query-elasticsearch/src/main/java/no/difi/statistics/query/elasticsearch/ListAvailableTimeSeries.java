package no.difi.statistics.query.elasticsearch;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import org.elasticsearch.client.Client;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static no.difi.statistics.model.MeasurementDistance.days;
import static no.difi.statistics.model.MeasurementDistance.hours;
import static no.difi.statistics.model.MeasurementDistance.minutes;
import static no.difi.statistics.model.MeasurementDistance.months;
import static no.difi.statistics.model.MeasurementDistance.years;

public class ListAvailableTimeSeries {

    private final static Pattern indexNamePattern = Pattern.compile("(.+):(.+):(minute|hour|day|month|year).*");
    private Client elasticSearchClient;

    private ListAvailableTimeSeries() {
        // Use builder
    }

    List<TimeSeriesDefinition> execute() {
        List<String> indices = asList(
                elasticSearchClient.admin().cluster()
                        .prepareState().execute()
                        .actionGet().getState()
                        .getMetaData().getConcreteAllIndices()
        );
        return indices.stream()
                .map(indexNamePattern::matcher)
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

        private Client elasticSearchClient;

        public Command elasticsearchClient(Client client) {
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
