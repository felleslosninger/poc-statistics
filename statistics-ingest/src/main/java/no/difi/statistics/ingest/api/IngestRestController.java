package no.difi.statistics.ingest.api;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.validation.ValidOrgno;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.view.RedirectView;
import springfox.documentation.annotations.ApiIgnore;

import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@Validated
@Api(tags = "Statistikk-inndata-api", description = "Legg data inn i statistikk-databasen")
@RestController
public class IngestRestController {
    private static final String DIGDIR_ORGNR = "991825827";
    private static final String OWNER_EXPLANATION = "eigar av tidsserien i form av eit organisasjonsnummer";
    private static final String SERIES_NAME_EXPLANATION = "tidsserier finnes ved oppslag i /meta";
    private static final String DISTANCE_EXPLANATION = "tidsserien sin måleavstand";

    private IngestService ingestService;

    public IngestRestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @ApiIgnore
    @GetMapping("/")
    public RedirectView index() {
        return new RedirectView("swagger-ui.html");
    }

    @ExceptionHandler(IngestService.TimeSeriesPointAlreadyExists.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public void alreadyExists() {
        // Do nothing
    }

    // TODO validate orgno
    @ApiOperation(value = "Legg inn data for ein tidsserie for din organisasjon. Organisasjonen må ha fått tilgong til dette i forkant i Maskinporten.")
    @PostMapping(
            value = "{owner}/{seriesName}/{distance}",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    @PreAuthorize("hasAuthority('SCOPE_digdir:statistikk.skriv')")
    public IngestResponse ingest(
            @ApiIgnore @AuthenticationPrincipal Jwt principal,
            @ApiParam(value = OWNER_EXPLANATION, example = DIGDIR_ORGNR, required = true)
            @PathVariable String owner, @ValidOrgno
            @ApiParam(value = SERIES_NAME_EXPLANATION, required = true)
            @PathVariable String seriesName,
            @ApiParam(value = DISTANCE_EXPLANATION, required = true)
            @PathVariable MeasurementDistance distance,
            @RequestBody List<TimeSeriesPoint> dataPoints
    ) {
        String authorizedOrgno = getOrgNoFromAuthorizedToken(principal);

        if (!owner.equals(authorizedOrgno)) {
            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN, "No access to orgno " + authorizedOrgno + " for timeseries owned by " + owner + ". Owner must be equal to authorized organization in Maskinporten.");
        }
        return ingestService.ingest(
                TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner),
                dataPoints
        );
    }

    // TODO improve code
    private String getOrgNoFromAuthorizedToken(Jwt principal) {
        final Map<String, Object> consumer = principal.getClaimAsMap("consumer");
        if(consumer==null || consumer.isEmpty() || consumer.get("ID") == null){
            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN, "Invalid access token, can not extract consumer orgno.");
        }
        String id = (String) consumer.get("ID");
        if(id==null||id.indexOf(":")<0 ){
            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN, "Invalid access token, can not extract consumer orgno.");
        }
        return id.substring(id.indexOf(":") + 1);
    }

    @ApiOperation(value = "Hent nyaste datapunkt frå ein tidsserie")
    @GetMapping("{owner}/{seriesName}/{distance}/last")
    public TimeSeriesPoint last(
            @ApiParam(value = OWNER_EXPLANATION, example = DIGDIR_ORGNR, required = true)
            @PathVariable @ValidOrgno String owner,
            @ApiParam(value = SERIES_NAME_EXPLANATION, required = true)
            @PathVariable String seriesName,
            @ApiParam(value = DISTANCE_EXPLANATION, required = true)
            @PathVariable MeasurementDistance distance,
            HttpServletResponse response
    ) {
        TimeSeriesPoint lastPoint = ingestService.last(TimeSeriesDefinition.builder().name(seriesName).distance(distance).owner(owner));
        if (lastPoint == null)
            response.setStatus(HttpStatus.NO_CONTENT.value());
        return lastPoint;
    }

}
