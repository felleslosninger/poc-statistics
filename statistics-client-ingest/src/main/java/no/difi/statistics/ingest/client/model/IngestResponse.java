package no.difi.statistics.ingest.client.model;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static no.difi.statistics.ingest.client.model.IngestResponse.Status.Ok;

@XmlRootElement
public class IngestResponse {

    public enum Status {Ok, Failed, Conflict}

    private List<Status> statuses = new ArrayList<>();

    private IngestResponse() {
        // Use builder
    }

    public boolean ok() {
        return statuses.stream().noneMatch(status -> status != Ok);
    }

    @XmlElement
    public List<Status> getStatuses() {
        return unmodifiableList(statuses);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private IngestResponse instance;

        Builder() {
            this.instance = new IngestResponse();
        }

        /**
         * Append status to the (ordered) list.
         */
        public Builder status(Status status) {
            instance.statuses.add(status);
            return this;
        }

        public IngestResponse build() {
            try {
                return instance;
            } finally {
                instance = null;
            }
        }

    }

}
