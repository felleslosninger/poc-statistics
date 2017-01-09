package no.difi.statistics.ingest.client;

import no.difi.statistics.ingest.client.model.TimeSeriesDefinition;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

import java.util.List;

public interface IngestService {

    void ingest(TimeSeriesDefinition seriesDefinition, TimeSeriesPoint timeSeriesPoint);

    void ingest(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> dataPoints);

    TimeSeriesPoint last(TimeSeriesDefinition seriesDefinition);

    class Failed extends RuntimeException {
        Failed() {
            super();
        }

        Failed(String message) {
            super(message);
        }

        Failed(String message, Throwable cause) {
            super(message, cause);
        }
    }

    class ConnectFailed extends Failed {

        ConnectFailed(Throwable cause) {
            super("Failed to connect", cause);
        }

    }

    class DataPointAlreadyExists extends Failed {
        DataPointAlreadyExists() {
            super();
        }

    }

    class MalformedUrl extends Failed {
        MalformedUrl(String message, Throwable cause) {
            super(message, cause);
        }
    }

    class Unauthorized extends Failed {
        Unauthorized(String message) {
            super(message);
        }
    }

}
