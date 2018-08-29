package no.difi.statistics.query.api;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import no.difi.statistics.model.*;
import no.difi.statistics.query.QueryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;
import springfox.documentation.annotations.ApiIgnore;

import java.time.ZonedDateTime;
import java.util.List;

import static java.lang.String.format;
import static no.difi.statistics.query.model.QueryFilter.queryFilter;

@Api(tags = "Statistics-query", description = "Hent ut data frå statistikk-databasen")
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

    @ApiIgnore
    @GetMapping("/")
    public RedirectView index() {
        return new RedirectView("swagger-ui.html");
    }

    @ApiOperation(value = "Hent ut liste over tilgjengelege tidsseriar")
    @GetMapping("/meta")
    public List<TimeSeriesDefinition> available() {
        return service.availableTimeSeries();
    }

    @ApiOperation(value = "Hent data frå ein tidsserie")
    @GetMapping("{owner}/{seriesName}/{distance}")
    public List<TimeSeriesPoint> query(
            @ApiParam(value = "eigar av tidsserien i form av eit organisasjonsnummer", example = "991825827", required = true)
            @PathVariable String owner,
            @PathVariable String seriesName,
            @ApiParam(value = "tidsserien sin måleavstand", required = true)
            @PathVariable MeasurementDistance distance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories,
            @RequestParam(required = false) String perCategory
    ) {
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.query(seriesDefinition, queryFilter().range(from, to).categories(categories).perCategory(perCategory).build());
    }

    @ApiOperation(value = "Hent nyaste datapunkt frå ein tidsserie")
    @GetMapping("{owner}/{seriesName}/{distance}/last")
    public TimeSeriesPoint last(
            @ApiParam(value = "eigar av tidsserien i form av eit organisasjonsnummer", example = "991825827", required = true)
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories
    ) {
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.last(seriesDefinition, queryFilter().range(from, to).categories(categories).build());
    }

    @ApiOperation(value = "Hent nyaste datapunkt frå ein tidsserie")
    @GetMapping("{owner}/{seriesName}/{distance}/last/{targetDistance}")
    public List<TimeSeriesPoint> lastHistogram(
            @ApiParam(value = "eigar av tidsserien i form av eit organisasjonsnummer", example = "991825827", required = true)
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @PathVariable MeasurementDistance targetDistance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories
    ) {
        validateMeasurementDistance(distance, targetDistance);
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.lastHistogram(seriesDefinition, targetDistance, queryFilter().range(from, to).categories(categories).build());
    }

    @ApiOperation(value = "Hent eitt datapunkt med sum av målingar",
        notes = "Returnerer eitt datapunkt")
    @GetMapping("{owner}/{seriesName}/{distance}/sum")
    public TimeSeriesPoint sum(
            @ApiParam(value = "eigar av tidsserien i form av eit organisasjonsnummer", example = "991825827", required = true)
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories
    ) {
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.sum(seriesDefinition, queryFilter().range(from, to).categories(categories).build());
    }

    @ApiOperation(value = "Hent datapunkter med summar av målingar, omforma til ny måleavstand",
        notes = "Ein tidsserie med måleavstand på timar kan for eksempel summerast opp på dag, månad eller årsnivå.")
    @GetMapping("{owner}/{seriesName}/{distance}/sum/{targetDistance}")
    public List<TimeSeriesPoint> sumHistogram(
            @ApiParam(value = "eigar av tidsserien i form av eit organisasjonsnummer", example = "991825827", required = true)
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @PathVariable MeasurementDistance targetDistance,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestParam(required = false) String categories
    ) {
        validateMeasurementDistance(distance, targetDistance);
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner);
        return service.sumHistogram(seriesDefinition, targetDistance, queryFilter().range(from, to).categories(categories).build());
    }

    @GetMapping(path = "{owner}/{seriesName}/{distance}/percentile", params = {"percentile", "measurementId", "operator"})
    @ApiOperation(value = "", notes = "<b>Experimental feature -- use at your own risk. Categorized series are not supported.</b>")
    public List<TimeSeriesPoint> relationalToPercentile(
            @ApiParam(value = "eigar av tidsserien i form av eit organisasjonsnummer", example = "991825827", required = true)
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
        return service.query(seriesDefinition, queryFilter().range(from, to).build(), new PercentileFilter(percentile, measurementId, operator));
    }

    private void validateMeasurementDistance(MeasurementDistance distance, MeasurementDistance targetDistance) {
        if (distance.ordinal() >= targetDistance.ordinal())
            throw new IllegalArgumentException(format("Distance %s is greater than or equal to target distance %s", distance, targetDistance));
    }

}
