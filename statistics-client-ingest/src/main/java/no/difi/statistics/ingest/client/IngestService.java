package no.difi.statistics.ingest.client;

import no.difi.statistics.ingest.client.model.IngestResponse;
import no.difi.statistics.ingest.client.model.TimeSeriesDefinition;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

import java.util.List;
import java.util.Optional;

public interface IngestService {

    IngestResponse ingest(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> dataPoints, String token);

    Optional<TimeSeriesPoint> last(TimeSeriesDefinition seriesDefinition);

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
        MalformedUrl(Throwable cause) {
            super("Malformed URL", cause);
        }
    }

    class Unauthorized extends Failed {
        Unauthorized(String message) {
            super(message);
        }
    }

}
