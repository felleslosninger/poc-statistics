package no.difi.statistics.ingest.client.exception;

public class MalformedUrl extends IngestFailed {
    public MalformedUrl(String message, Throwable cause) {
        super(message, cause);
    }
}
