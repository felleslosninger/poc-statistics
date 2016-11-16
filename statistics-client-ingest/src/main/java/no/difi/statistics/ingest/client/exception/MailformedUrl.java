package no.difi.statistics.ingest.client.exception;

public class MailformedUrl extends RuntimeException{

    public MailformedUrl(String message){
        super(message);
    }

    public MailformedUrl(String message, Throwable cause) {
        super(message, cause);
    }
}
