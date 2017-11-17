package no.difi.statistics.ingest.poc;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import javax.json.*;
import java.io.InputStream;
import java.net.URL;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static no.difi.statistics.model.MeasurementDistance.minutes;

public class DifiAdminIngester implements ApplicationRunner {

    private final static String urlTemplate =
            "https://admin.difi.eon.no/idporten-admin/statistics/statistics/json/r1/%s/to/%s";
    private final static DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm");
    private final static String difiOrganizationNumber = "991825827";
    private IngestService service;

    public DifiAdminIngester(IngestService service) {
        this.service = service;
    }

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        if (!applicationArguments.containsOption("from")) return;
        ApplicationArgumentsReader argumentsReader = new ApplicationArgumentsReader(applicationArguments);
        for (ZonedDateTime t = argumentsReader.from(); t.isBefore(argumentsReader.to()); t = t.plusMinutes(1)) {
            URL url = new URL(format(urlTemplate, dtFormatter.format(t), dtFormatter.format(t.plusMinutes(1))));
            try (InputStream response = url.openStream()) {
                JsonReader reader = Json.createReader(response);
                TimeSeriesPoint.Builder pointBuilder = TimeSeriesPoint.builder().timestamp(t);
                for (JsonValue jsonValue : reader.readArray()) {
                    JsonArray fields = ((JsonObject)jsonValue).getJsonArray("fields");
                    measurements(pointBuilder, fields);
                }
                service.ingest(
                        TimeSeriesDefinition.builder().name(timeSeriesName()).distance(minutes).owner(difiOrganizationNumber),
                        singletonList(pointBuilder.build())
                );
            }
        }
    }

    private String timeSeriesName() {
        return "idporten-login";
    }

    private void measurements(TimeSeriesPoint.Builder pointBuilder, JsonArray fields) {
        pointBuilder
                .measurement(measurement("MinID", 4, fields))
                .measurement(measurement("MinID OTC", 5, fields))
                .measurement(measurement("MinID PIN", 6, fields))
                .measurement(measurement("BuyPass", 7, fields))
                .measurement(measurement("Commfides", 8, fields))
                .measurement(measurement("Federated", 9, fields))
                .measurement(measurement("BankID", 10, fields))
                .measurement(measurement("eIDAS", 11, fields))
                .measurement(measurement("BankID mobil", 12, fields))
                .measurement(measurement("Alle", 13, fields));
    }

    private Measurement measurement(String authenticationMethod, int index, JsonArray fields) {
        return new Measurement(
                format("%s/%s", authenticationMethod, stringValue(0, fields)),
                longValue(index, fields)
        );
    }

    private long longValue(int index, JsonArray fields) {
        return fields.getJsonObject(index).getJsonNumber("value").longValueExact();
    }

    private String stringValue(int index, JsonArray fields) {
        return fields.getJsonObject(index).getString("value");
    }

}
