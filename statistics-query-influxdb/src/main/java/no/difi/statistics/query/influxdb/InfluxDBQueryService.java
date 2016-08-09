package no.difi.statistics.query.influxdb;

import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.QueryService;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;

public class InfluxDBQueryService implements QueryService {

    private InfluxDB client;

    public InfluxDBQueryService(InfluxDB client) {
        this.client = client;
    }

    @Override
    public List<TimeSeriesPoint> minutes(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        QueryResult result = query().from(seriesName).timeRange(from, to).execute();
        return convert(result);
    }

    @Override
    public List<TimeSeriesPoint> minutes(String seriesName, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter) {
        double percentileValue = percentileValue(seriesName, from, to, filter);
        QueryResult result = query().from(seriesName).timeRange(from, to).greaterThan(filter.getMeasurementId(), percentileValue).execute();
        return convert(result);
    }

    private double percentileValue(String seriesName, ZonedDateTime from, ZonedDateTime to, TimeSeriesFilter filter) {
        QueryResult result = query()
                        .selectPercentileValue(filter.getMeasurementId(), filter.getPercentile())
                        .from(seriesName)
                        .timeRange(from, to)
                        .execute();
        return (Double)series(result)
                .map(s -> s.getValues().get(0).get(1))
                .orElseThrow(() -> new RuntimeException("Failed to calculate percentile value"));
    }

    @Override
    public List<TimeSeriesPoint> hours(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TimeSeriesPoint> days(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TimeSeriesPoint> months(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TimeSeriesPoint> years(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TimeSeriesPoint point(String seriesName, ZonedDateTime from, ZonedDateTime to) {
        throw new UnsupportedOperationException();
    }

    private List<TimeSeriesPoint> convert(QueryResult result) {
        return series(result).map(this::convert).orElse(emptyList());
    }

    private List<TimeSeriesPoint> convert(QueryResult.Series series) {
        List<TimeSeriesPoint> points = new ArrayList<>();
        for (int i = 0; i < series.getValues().size(); i++) {
            TimeSeriesPoint.Builder pointBuilder = TimeSeriesPoint.builder().timestamp(ZonedDateTime.parse(series.getValues().get(i).get(0).toString()));
            for (int j = 1; j < series.getValues().get(i).size(); j++) {
                pointBuilder.measurement(series.getColumns().get(j), ((Double) series.getValues().get(i).get(j)).intValue());
            }
            points.add(pointBuilder.build());
        }
        return points;
    }

    private Optional<QueryResult.Series> series(QueryResult result) {
        if (result.getError() != null)
            throw new RuntimeException("Query failed: " + result.getError());
        if (result.getResults().size() != 1)
            throw new RuntimeException("Query result size is not 1 but " + result.getResults().size());
        if (result.getResults().get(0).getSeries() == null)
            return Optional.empty();
        if (result.getResults().get(0).getSeries().size() != 1)
            throw new RuntimeException("Number of series in query result is not 1 but " + result.getResults().get(0).getSeries().size());
        return Optional.of(result.getResults().get(0).getSeries().get(0));
    }

    private InfluxQuery query() {
        return new InfluxQuery(client);
    }

    private static class InfluxQuery {

        private String selectClause = "SELECT *";
        private String fromClause;
        private List<String> filters = new ArrayList<>();
        private String groupByClause = "";
        private InfluxDB client;
        private final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

        InfluxQuery(InfluxDB client) {
            this.client = client;
        }

        InfluxQuery selectPercentileValue(String measurementId, int percentile) {
            this.selectClause = format("SELECT PERCENTILE(%s, %d)", measurementId, percentile);
            return this;
        }

        InfluxQuery from(String timeSeriesName) {
            this.fromClause = "FROM " + timeSeriesName;
            return this;
        }

        InfluxQuery greaterThan(String measurementId, double value) {
            this.filters.add(format(Locale.ROOT, "%s > %f", measurementId, value));
            return this;
        }

        InfluxQuery timeRange(ZonedDateTime from, ZonedDateTime to) {
            this.filters.add(format("time >= '%s' AND time <= '%s'", formatTime(from), formatTime(to)));
            return this;
        }

        InfluxQuery groupByMonth() {
            this.groupByClause = "GROUP BY count,time(1M)"; // Month not supported
            this.selectClause = "SELECT SUM(count)";
            return this;
        }

        private static String formatTime(ZonedDateTime time) {
            return dateTimeFormatter.format(time.truncatedTo(ChronoUnit.MILLIS));
        }

        private static String whereClause(List<String> filters) {
            if (filters.isEmpty())
                return "";
            return "WHERE " + filters.stream().collect(joining(" AND "));
        }

        QueryResult execute() {
            return client.query(new Query(selectClause + " " + fromClause + " " + whereClause(filters) + " " + groupByClause, "default"));
        }

    }


}
