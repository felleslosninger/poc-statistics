package no.difi.statistics.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;

@JsonDeserialize(using = Measurement.MeasurementJsonDeserializer.class)
public class Measurement {

    private String id;
    private int value;

    public Measurement(String id, int value) {
        this.id = id;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public int getValue() {
        return value;
    }

    static class MeasurementJsonDeserializer extends JsonDeserializer<Measurement> {

        @Override
        public Measurement deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
            JsonNode node = parser.getCodec().readTree(parser);
            return new Measurement(node.get("id").asText(), node.get("value").asInt());
        }

    }

}
