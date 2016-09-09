package no.difi.statistics.ingest.api;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.http.MediaType;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import static java.lang.String.format;

@RestController
public class IngestRestController {

    private IngestService ingestService;

    public IngestRestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @GetMapping(value="/")
    public String index() {
        return format(
                "Statistics Ingest version %s",
                System.getProperty("difi.version", "N/A")
        );
    }

    @PostMapping(
            value = "minutes/{owner}/{seriesName}",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public void minute(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestBody TimeSeriesPoint dataPoint
    ) {
        ingestService.minute(seriesName, owner, dataPoint);
    }
}
