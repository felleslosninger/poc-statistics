package no.difi.statistics.ingest.client;

import no.difi.statistics.ingest.client.exception.IngestException;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

import java.io.IOException;

public interface IngestService {

    void minute(String timeSeriesName, TimeSeriesPoint dataPoint) throws IngestException;

}
