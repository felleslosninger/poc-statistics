package no.difi.statistics.ingest.client;

public enum Series {
    MINUTE("minute"),
    HOUR("hour");

    private final String serie;

    Series(String serie) {
        this.serie = serie;
    }

    public String getSerie() {
        return serie;
    }
}
