package no.difi.statistics.query.api;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.QueryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

@RestController
public class QueryRestController {

    private QueryService service;

    public QueryRestController(QueryService service) {
        this.service = service;
    }

    @GetMapping("/")
    public RedirectView index() throws IOException {
        return new RedirectView("swagger-ui.html");
    }

    @GetMapping("{owner}/minutes")
    public List<String> timeSeries(
        @PathVariable String owner)
    {
        return service.availableTimeSeries(owner);
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

    @PostMapping("{owner}/{seriesName}/minutes")
    public List<TimeSeriesPoint> minutesAbovePercentile(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestBody TimeSeriesFilter filter
    ) {
        return service.minutes(seriesName, owner, from, to, filter);
    }

    @GetMapping("{owner}/{seriesName}/minutes/last/months")
    public List<TimeSeriesPoint> lastInMonths(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.lastInMonths(seriesName, owner, from, to);
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

}
