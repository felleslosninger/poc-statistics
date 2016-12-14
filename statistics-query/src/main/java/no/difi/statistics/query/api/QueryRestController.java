package no.difi.statistics.query.api;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.RelationalOperator;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.QueryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

import static java.lang.String.format;

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
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    )
    {
        return service.last(seriesName, distance, owner, from, to);
    }

    @GetMapping("{owner}/{seriesName}/{distance}")
    public List<TimeSeriesPoint> query(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        switch (distance) {
            case minutes: return service.minutes(seriesName, owner, from, to);
            case hours: return service.hours(seriesName, owner, from, to);
            case days: return service.days(seriesName, owner, from, to);
            case months: return service.months(seriesName, owner, from, to);
            case years: return service.years(seriesName, owner, from, to);
            default: throw new IllegalArgumentException(distance.toString());
        }
    }

    @GetMapping(path = "{owner}/{seriesName}/{distance}", params = {"percentile", "measurementId", "operator"})
    public List<TimeSeriesPoint> relationalToPercentile(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam int percentile,
            @RequestParam String measurementId,
            @RequestParam RelationalOperator operator
    ) {
        return service.query(seriesName, distance, owner, from, to, new TimeSeriesFilter(percentile, measurementId, operator));
    }

    @GetMapping("{owner}/{seriesName}/{distance}/last/{targetDistance}")
    public List<TimeSeriesPoint> lastPerDistance(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @PathVariable MeasurementDistance targetDistance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        if (distance.ordinal() > targetDistance.ordinal())
            throw new IllegalArgumentException(format("Distance %s is greater than target distance %s", distance, targetDistance));
        return service.lastPerDistance(seriesName, distance, targetDistance, owner, from, to);
    }

    @GetMapping("{owner}/{seriesName}/{distance}/sum")
    public TimeSeriesPoint sum(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.sum(seriesName, distance, owner, from, to);
    }

    @GetMapping("{owner}/{seriesName}/{distance}/sum/{targetDistance}")
    public List<TimeSeriesPoint> sumPerDistance(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @PathVariable MeasurementDistance targetDistance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.sumPerDistance(seriesName, distance, targetDistance, owner, from, to);
    }

}
