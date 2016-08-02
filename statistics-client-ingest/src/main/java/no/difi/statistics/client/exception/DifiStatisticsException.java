package no.difi.statistics.client.exception;

public class DifiStatisticsException extends Exception{

    public DifiStatisticsException(String message){
        super(message);
    }

    public DifiStatisticsException(String message, Throwable cause) {
        super(message, cause);
    }
}
