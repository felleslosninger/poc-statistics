package no.difi.statistics.ingest.influxdb;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBIngestService implements IngestService {

    private InfluxDB client;

    public InfluxDBIngestService(InfluxDB client) {
        this.client = client;
    }

    @Override
    public void minute(String timeSeriesName, TimeSeriesPoint dataPoint) {
        client.createDatabase(timeSeriesName);
        Point.Builder influxPoint = Point.measurement("tom")
                .time(dataPoint.getTimestamp().toInstant().toEpochMilli(), TimeUnit.MILLISECONDS);
        for (Measurement measurement : dataPoint.getMeasurements())
            influxPoint.addField(measurement.getId(), measurement.getValue());
        client.write(timeSeriesName, "default", influxPoint.build());
    }

}
