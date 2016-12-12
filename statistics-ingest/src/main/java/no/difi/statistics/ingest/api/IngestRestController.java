package no.difi.statistics.ingest.api;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
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
            value = "{owner}/{seriesName}/{distance}",
            params = "!bulk",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public void ingest(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestBody TimeSeriesPoint dataPoint
    ) {
        ingestService.ingest(TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner), dataPoint);
    }

    @PostMapping(
            value = "{owner}/{seriesName}/{distance}",
            params = "bulk",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public IngestResponse bulkIngest(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestBody List<TimeSeriesPoint> dataPoints
    ) {
        return ingestService.ingest(TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner), dataPoints);
    }

    @GetMapping("{owner}/{seriesName}/{distance}/last")
    public TimeSeriesPoint last(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance
    ) {
        return ingestService.last(TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner));
    }

}
