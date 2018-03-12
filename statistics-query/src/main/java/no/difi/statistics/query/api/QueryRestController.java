package no.difi.statistics.query.api;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.RelationalOperator;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.PercentileFilter;
import no.difi.statistics.query.QueryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

import static java.lang.String.format;
import static no.difi.statistics.model.query.QueryFilter.queryFilter;

@RestController
public class QueryRestController {

    private QueryService service;

    public QueryRestController(QueryService service) {
        this.service = service;
    }

    @ExceptionHandler
    @ResponseStatus
    public String handle(Exception e) {
        return e.getMessage();
    }

    @GetMapping("/")
    public RedirectView index() throws IOException {
        return new RedirectView("swagger-ui.html");
    }

    @GetMapping("/meta")
    public List<TimeSeriesDefinition> available() {
        return service.availableTimeSeries();
    }

    @GetMapping("{owner}/{seriesName}/{distance}/last")
    public TimeSeriesPoint last(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories
    )
    {
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.last(seriesDefinition, queryFilter().from(from).to(to).categories(categories).build());
    }

    @GetMapping("{owner}/{seriesName}/{distance}")
    public List<TimeSeriesPoint> query(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories
    ) {
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.query(seriesDefinition, queryFilter().from(from).to(to).categories(categories).build());
    }

    @GetMapping(path = "{owner}/{seriesName}/{distance}/percentile", params = {"percentile", "measurementId", "operator"})
    public List<TimeSeriesPoint> relationalToPercentile(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories,
            @RequestParam int percentile,
            @RequestParam String measurementId,
            @RequestParam RelationalOperator operator
    ) {
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.query(seriesDefinition, queryFilter().from(from).to(to).build(), new PercentileFilter(percentile, measurementId, operator));
    }

    @GetMapping("{owner}/{seriesName}/{distance}/last/{targetDistance}")
    public List<TimeSeriesPoint> lastPerDistance(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @PathVariable MeasurementDistance targetDistance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories
    ) {
        if (distance.ordinal() > targetDistance.ordinal())
            throw new IllegalArgumentException(format("Distance %s is greater than target distance %s", distance, targetDistance));
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.lastPerDistance(seriesDefinition, targetDistance, queryFilter().from(from).to(to).categories(categories).build());
    }

    @GetMapping("{owner}/{seriesName}/{distance}/sum")
    public TimeSeriesPoint sum(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories
    ) {
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.sum(seriesDefinition, queryFilter().from(from).to(to).categories(categories).build());
    }

    @GetMapping("{owner}/{seriesName}/{distance}/sum/{targetDistance}")
    public List<TimeSeriesPoint> sumPerDistance(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @PathVariable MeasurementDistance targetDistance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories
    ) {
        if (distance.ordinal() > targetDistance.ordinal())
            throw new IllegalArgumentException(format("Distance %s is greater than target distance %s", distance, targetDistance));
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.sumPerDistance(seriesDefinition, targetDistance, queryFilter().from(from).to(to).categories(categories).build());
    }

}
