package no.difi.statistics.ingest.influxdb;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.TimeSeriesPoint;
import org.influxdb.InfluxDB;

public class InfluxDBIngestService implements IngestService {

    private InfluxDB client;

    public InfluxDBIngestService(InfluxDB client) {
        this.client = client;
    }

    @Override
    public void minute(String timeSeriesName, TimeSeriesPoint dataPoint) {
        client.describeDatabases(); // TODO
    }

}
