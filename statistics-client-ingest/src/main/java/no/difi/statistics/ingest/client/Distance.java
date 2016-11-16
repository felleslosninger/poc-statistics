package no.difi.statistics.ingest.client;

public enum Distance {
    MINUTE("minute"),
    HOUR("hour");

    private final String serie;

    Distance(String serie) {
        this.serie = serie;
    }

    public String getSerie() {
        return serie;
    }
}
