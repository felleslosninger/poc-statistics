package no.difi.statistics.ingest.influxdb;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBIngestService implements IngestService {

    private InfluxDB client;
    private static final String databaseName = "default";

    public InfluxDBIngestService(InfluxDB client) {
        this.client = client;
    }

    @Override
    public void minute(String timeSeriesName, TimeSeriesPoint dataPoint) {
        client.createDatabase(databaseName); // Does a CREATE DATABASE IF NOT EXISTS
        Point.Builder influxPoint = Point.measurement(timeSeriesName)
                .time(dataPoint.getTimestamp().toInstant().toEpochMilli(), TimeUnit.MILLISECONDS);
        for (Measurement measurement : dataPoint.getMeasurements())
            influxPoint.addField(measurement.getId(), measurement.getValue());
        client.write(databaseName, null, influxPoint.build());
    }

}
