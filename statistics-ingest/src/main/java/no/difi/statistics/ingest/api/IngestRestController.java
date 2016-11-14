package no.difi.statistics.ingest.api;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;
import java.util.List;

@RestController
public class IngestRestController {

    private IngestService ingestService;

    public IngestRestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @GetMapping("/")
    public RedirectView index() throws IOException {
        return new RedirectView("swagger-ui.html");
    }

    @ExceptionHandler(IngestService.TimeSeriesPointAlreadyExists.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public void alreadyExists() {
        // Do nothing
    }

    @PostMapping(
            value = "{owner}/{seriesName}/minute",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public void minute(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestBody TimeSeriesPoint dataPoint
    ) {
        ingestService.minute(seriesName, owner, dataPoint);
    }

    @PostMapping(
            value = "{owner}/{seriesName}/minutes",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public IngestResponse minutes(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestBody List<TimeSeriesPoint> dataPoints
    ) {
        return ingestService.minutes(seriesName, owner, dataPoints);
    }

    @PostMapping(
            value = "{owner}/{seriesName}/hour",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public void hour(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestBody TimeSeriesPoint dataPoint
    ) {
        ingestService.hour(seriesName, owner, dataPoint);
    }
}
