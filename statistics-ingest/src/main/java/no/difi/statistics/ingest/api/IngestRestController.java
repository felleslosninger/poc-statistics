package no.difi.statistics.ingest.api;

import no.difi.statistics.ingest.IngestService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

import static java.lang.String.format;

@RestController
public class IngestRestController {

    private IngestService ingestService;

    public IngestRestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @GetMapping("/")
    public String index() throws IOException {
        return format(
                "Statistics Ingest version %s",
                System.getProperty("difi.version", "N/A")
        );
    }

}
