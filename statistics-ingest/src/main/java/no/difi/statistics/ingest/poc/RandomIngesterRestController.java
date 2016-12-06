package no.difi.statistics.ingest.poc;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.time.temporal.ChronoUnit.*;
import static java.time.temporal.ChronoUnit.YEARS;

@RestController
public class RandomIngesterRestController {

    private IngestService service;

    public RandomIngesterRestController(IngestService service) {
        this.service = service;
    }

    @RequestMapping(
            method = RequestMethod.POST,
            value = "{owner}/{seriesName}/{distance}",
            params = {"from", "to", "random"},
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public void minutes(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @PathVariable MeasurementDistance distance,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        Random random = new Random();
        List<TimeSeriesPoint> points = new ArrayList<>();
        for (ZonedDateTime t = from; t.isBefore(to); t = t.plus(1, unit(distance))) {
            points.add(TimeSeriesPoint.builder().timestamp(t).measurement("count", random.nextInt(1_000_000)).build());
        }
        service.ingest(
                TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner),
                points
        );
    }

    public static ChronoUnit unit(MeasurementDistance distance) {
        switch (distance) {
            case minutes: return MINUTES;
            case hours: return HOURS;
            case days: return DAYS;
            case months: return MONTHS;
            case years: return YEARS;
            default: throw new IllegalArgumentException(distance.toString());
        }
    }

}
