package no.difi.statistics.ingest.client.exception;

public class MissingData extends RuntimeException {
    public MissingData(String message) {
        super(message);
    }
}
