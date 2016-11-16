package no.difi.statistics.ingest.client;

public enum Distance {
    minute("minute"),
    hour("hour");

    private final String value;

    Distance(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
