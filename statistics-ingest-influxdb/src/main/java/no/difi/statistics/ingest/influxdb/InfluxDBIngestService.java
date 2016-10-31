package no.difi.statistics.ingest.influxdb;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.ingest.IngestResponse;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class InfluxDBIngestService implements IngestService {

    private InfluxDB client;

    public InfluxDBIngestService(InfluxDB client) {
        this.client = client;
    }

    @Override
    public void minute(String timeSeriesName, String owner, TimeSeriesPoint dataPoint) {
        client.createDatabase(owner); // Does a CREATE DATABASE IF NOT EXISTS
        Point.Builder influxPoint = Point.measurement(format("%s", timeSeriesName))
                .time(dataPoint.getTimestamp().toInstant().toEpochMilli(), TimeUnit.MILLISECONDS);
        for (Measurement measurement : dataPoint.getMeasurements())
            influxPoint.addField(measurement.getId(), measurement.getValue());
        client.write(owner, null, InfluxDB.ConsistencyLevel.ALL, influxPoint.build().lineProtocol());
    }

    @Override
    public IngestResponse minutes(String timeSeriesName, String owner, List<TimeSeriesPoint> dataPoints) {
        throw new UnsupportedOperationException();
    }

}
