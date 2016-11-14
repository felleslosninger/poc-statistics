package no.difi.statistics.ingest.client;

import no.difi.statistics.ingest.client.exception.IngestException;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

public interface IngestService {

    void minute(String timeSeriesName, TimeSeriesPoint dataPoint) throws IngestException;
    void hour(String timeSeriesName, TimeSeriesPoint dataPoint) throws IngestException;

}
