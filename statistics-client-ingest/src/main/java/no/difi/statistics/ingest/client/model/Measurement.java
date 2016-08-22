package no.difi.statistics.ingest.client.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;

@JsonDeserialize(using = Measurement.MeasurementJsonDeserializer.class)
@XmlRootElement
public class Measurement {

    private final String id;
    private final int value;

    public Measurement(String id, int value) {
        this.id = id;
        this.value = value;
    }

    @XmlElement
    public String getId() {
        return id;
    }

    @XmlElement
    public int getValue() {
        return value;
    }

    /**
     * Use custom deserializer to maintain immutability property
     */
    static class MeasurementJsonDeserializer extends JsonDeserializer<Measurement> {

        @Override
        public Measurement deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
            JsonNode node = parser.getCodec().readTree(parser);
            return new Measurement(node.get("id").asText(), node.get("value").asInt());
        }
    }

    @Override
    public String toString() {
        return "Measurement{" +
                "id='" + id + '\'' +
                ", value=" + value +
                "}";
    }

}
