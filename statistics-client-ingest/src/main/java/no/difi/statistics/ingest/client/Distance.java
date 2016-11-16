package no.difi.statistics.ingest.client;

public enum Distance {
    minute("minute"),
    hour("hour");

    private final String serie;

    Distance(String serie) {
        this.serie = serie;
    }

    public String getSerie() {
        return serie;
    }
}
