package no.difi.statistics.api;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static no.difi.statistics.util.IndexNameResolver.resolveDayIndexNames;
import static no.difi.statistics.util.IndexNameResolver.resolveHourIndexNames;
import static no.difi.statistics.util.IndexNameResolver.resolveMinuteIndexNames;
import static no.difi.statistics.util.IndexNameResolver.resolveMonthIndexNames;
import static no.difi.statistics.util.IndexNameResolver.resolveYearIndexNames;

public class Statistics {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String timeFieldName = "timestamp";

    private Client elasticSearchClient;
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public Statistics(Client elasticSearchClient) {
        this.elasticSearchClient = elasticSearchClient;
    }

    public List<TimeSeriesPoint> minutes(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveMinuteIndexNames(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> hours(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveHourIndexNames(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> days(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveDayIndexNames(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> months(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveMonthIndexNames(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> years(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveYearIndexNames(seriesName, from, to), type, from, to);
    }

    private List<TimeSeriesPoint> search(List<String> indexNames, String type, ZonedDateTime from, ZonedDateTime to) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(
                    "Executing search:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                    indexNames.stream().collect(joining(",\n  ")),
                    type,
                    from,
                    to
            ));
        }
        SearchResponse response = elasticSearchClient
                .prepareSearch(indexNames.stream().collect(joining(",")))
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .setTypes(type)
                .setQuery(QueryBuilders.rangeQuery(timeFieldName).from(dateTimeFormatter.format(from)).to(dateTimeFormatter.format(to)))
                .addSort(timeFieldName, SortOrder.ASC)
                .setSize(10_000) // 10 000 is maximum
                .execute().actionGet();
        List<TimeSeriesPoint> series = new ArrayList<>();
        if (logger.isDebugEnabled()) {
            logger.debug("Search result:\n" + response);
        }
        for (SearchHit hit : response.getHits()) {
            series.add(point(hit));
        }
        return series;
    }

    private TimeSeriesPoint point(SearchHit hit) {
        return TimeSeriesPoint.builder().timestamp(time(hit)).measurements(measurements(hit)).build();
    }

    private ZonedDateTime time(SearchHit hit) {
        return ZonedDateTime.parse(hit.getSource().get(timeFieldName).toString(), dateTimeFormatter);
    }

    private List<Measurement> measurements(SearchHit hit) {
        List<Measurement> measurements = new ArrayList<>();
        hit.getSource().keySet().stream().filter(field -> !field.equals(timeFieldName)).forEach(field -> {
            int value = Integer.valueOf(hit.getSource().get(field).toString());
            measurements.add(new Measurement(field, value));
        });
        return measurements;
    }

}
