package no.difi.statistics.ingest.client;

public enum Distance {
    minute("minutes"),
    hour("hours");

    private final String value;

    Distance(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
