package no.difi.statistics.query.config;

import no.difi.statistics.query.api.QueryRestController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.Ordered;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.handler.SimpleUrlHandlerMapping;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.time.ZonedDateTime;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static springfox.documentation.builders.PathSelectors.any;
import static springfox.documentation.builders.RequestHandlerSelectors.basePackage;

@SpringBootApplication
@EnableSwagger2
@PropertySource("classpath:application.properties")
public class AppConfig {

    private final BackendConfig backendConfig;

    @Autowired
    public AppConfig(BackendConfig backendConfig) {
        this.backendConfig = backendConfig;
    }

    @Bean
    public QueryRestController api() {
        return new QueryRestController(backendConfig.queryService());
    }

    @Bean
    public Docket apiDocumentation() {
        final String apiVersion = System.getProperty("difi.version", "N/A");
        return new Docket(DocumentationType.SWAGGER_2)
                .groupName("statistikk-utdata")
                .directModelSubstitute(ZonedDateTime.class, java.util.Date.class)
                .select()
                    .apis(basePackage(QueryRestController.class.getPackage().getName()))
                    .paths(any())
                    .build()
                .genericModelSubstitutes(Optional.class)
                .apiInfo(new ApiInfoBuilder()
                        .title("Statistikk for offentlege tenester")
                        .description(
                                format(
                                        "Skildring av API for uthenting av data (versjon %s).\n\n"
                                                + "<i>Tidsserie og måleavstand</i>\n"
                                                + "Ein tidsserie er identifisert av kombinasjonen av dei tre sti-parametera eigar, tidsserienamn og måleavstand. Ein tidsserie er lagra med ein spesifikk måleavstand (for eksempel 'hours'). Det er mogleg å konvertere til ein høgre måleavstand, til dømes frå 'hours' til 'months', i dei endepunkta med sti-parameteret 'targetDistance'. Moglege verdiar for måleavstand er 'minutes', 'hours', 'days', 'months', 'years'.\n\n"
                                                + "<i>Kategoriar</i>\n"
                                                + "Med parameteret 'categories' kan du filtrere data ut frå kategoriar oppgjevne på enkeltmålingane. For eksempel i statistikk for idporten-innloggingar, kan du hente ut datapunkt som kun tek med målingar der tenesteeigar er Skatteetaten.\n\n'"
                                                + "Merk at filteret ikkje filtrerer der det er nøyaktig lik verdi. Til dømes blir søketermen 'https://www.vest-testen.kommune.no/v3/' tolka til fleire søkeord (tokens): 'https', 'www.vest', 'testen.kommune.no' og 'v3'. Filteret sjekkar at alle søkeordene er med, men utelet ikkje treff som også har fleire søkeord. I dette dømet vert også 'https://www.vest-testen.kommune.no/sd/v3/' tatt med ('sd' er eit ekstra søkeord).\n\n"
                                        + "<i>Per kategorinøkkel</i>\n"
                                        + "Med parameteret 'perCategory' kan du hente ut datapunkt for kvar ulik verdi på kategorinøkkelen du oppgir. For eksempel, med statistikk for idporten-innloggingar, kan du få fleire datapunkt på samme tid, der kvart datapunkt er for ulike verdiar av Tjenesteeigar (kategorinøkkel). I dette eksempelet kan ulike verdiar av Tjenesteeigar kan vere Skatteetaten, Aure kommune etc.\n\n"
                                        + "<i>Tidspunkt</i>\n"
                                        + "Alle tidspunkt i parameter og responsar er oppgjevne i ISO 8601 datetime-format. Eksempel: '2018-06-18T09:00Z'."

                                        ,
                                        apiVersion
                                )
                        )
                        .version(apiVersion)
                        .build()
                );
    }

    @Bean
    public HandlerMapping specificWebjarHandlerMappingForSwaggerWithPrecedenceToAvoidClashWithControllerRequestMapping() {
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setOrder(Ordered.HIGHEST_PRECEDENCE);
        mapping.setUrlMap(singletonMap("/webjars/**", webjarRequestHandler()));
        return mapping;
    }

    @Bean
    public ResourceHttpRequestHandler webjarRequestHandler() {
        ResourceHttpRequestHandler requestHandler = new ResourceHttpRequestHandler();
        requestHandler.setLocations(singletonList(new ClassPathResource("META-INF/resources/webjars/")));
        return requestHandler;
    }

}
