package no.difi.statistics.api;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static no.difi.statistics.util.IndexNameResolver.resolveDaySeries;
import static no.difi.statistics.util.IndexNameResolver.resolveHourSeries;
import static no.difi.statistics.util.IndexNameResolver.resolveMinuteSeries;
import static no.difi.statistics.util.IndexNameResolver.resolveMonthSeries;
import static no.difi.statistics.util.IndexNameResolver.resolveYearSeries;

public class Statistics {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String timeFieldName = "timestamp";

    private Client elasticSearchClient;
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public Statistics(Client elasticSearchClient) {
        this.elasticSearchClient = elasticSearchClient;
    }

    public List<TimeSeriesPoint> minutes(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveMinuteSeries(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> hours(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveHourSeries(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> days(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveDaySeries(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> months(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveMonthSeries(seriesName, from, to), type, from, to);
    }

    public List<TimeSeriesPoint> years(String seriesName, String type, ZonedDateTime from, ZonedDateTime to) {
        return search(resolveYearSeries(seriesName, from, to), type, from, to);
    }

    private List<TimeSeriesPoint> search(List<String> indexNames, String type, ZonedDateTime from, ZonedDateTime to) {
        logger.info(format(
                "Executing search:\nIndexes: %s\nType: %s\nFrom: %s\nTo: %s\n",
                indexNames.stream().collect(joining(",\n  ")),
                type,
                from,
                to
        ));
        SearchResponse response = elasticSearchClient
                .prepareSearch(indexNames.stream().collect(joining(",")))
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .setTypes(type)
                .addField(timeFieldName).addField("value")
                .setQuery(QueryBuilders.rangeQuery(timeFieldName).from(dateTimeFormatter.format(from)).to(dateTimeFormatter.format(to)))
                .addSort(timeFieldName, SortOrder.ASC)
                .setSize(10_000) // 10 000 is maximum
                .execute().actionGet();
        List<TimeSeriesPoint> series = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            series.add(point(hit));
        }
        return series;
    }

    private TimeSeriesPoint point(SearchHit hit) {
        return TimeSeriesPoint.builder().timestamp(time(hit)).measurements(measurements(hit)).build();
    }

    private ZonedDateTime time(SearchHit hit) {
        return ZonedDateTime.parse(hit.field(timeFieldName).value(), dateTimeFormatter);
    }

    private List<Measurement> measurements(SearchHit hit) {
        List<Measurement> measurements = new ArrayList<>();
        for (SearchHitField hitField : hit) {
            if (!hitField.name().equals(timeFieldName))
                measurements.add(new Measurement(hitField.name(), hitField.value()));
        }
        return measurements;
    }

}
