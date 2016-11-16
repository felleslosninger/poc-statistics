package no.difi.statistics.ingest.client.exception;

public class IngestException extends RuntimeException {
    public IngestException(String message) {
        super(message);
    }

    public IngestException(String message, Throwable cause) {
        super(message, cause);
    }
}
