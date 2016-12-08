package no.difi.statistics.ingest.client.exception;

public class IngestFailed extends RuntimeException {
    IngestFailed() {
        super();
    }

    public IngestFailed(String message) {
        super(message);
    }

    public IngestFailed(String message, Throwable cause) {
        super(message, cause);
    }
}
