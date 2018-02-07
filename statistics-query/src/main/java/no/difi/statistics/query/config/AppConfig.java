package no.difi.statistics.query.config;

import no.difi.statistics.query.api.QueryRestController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
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

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static springfox.documentation.builders.PathSelectors.any;
import static springfox.documentation.builders.RequestHandlerSelectors.basePackage;

@SpringBootApplication
@EnableSwagger2
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
        return new Docket(DocumentationType.SWAGGER_2)
                .groupName("statistikk-utdata")
                .directModelSubstitute(ZonedDateTime.class, java.util.Date.class)
                .select()
                    .apis(basePackage(QueryRestController.class.getPackage().getName()))
                    .paths(any())
                    .build()
                .apiInfo(new ApiInfoBuilder()
                        .title("Statistikk for offentlige tjenester")
                        .description(
                                format(
                                        "Beskrivelse av API for uthenting av data (versjon %s).",
                                        System.getProperty("difi.version", "N/A")
                                )
                        )
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
