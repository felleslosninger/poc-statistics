package no.difi.statistics.ingest.config;

import com.google.common.base.Predicates;
import no.difi.statistics.ingest.api.IngestRestController;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.handler.SimpleUrlHandlerMapping;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.OAuthBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.service.*;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spi.service.contexts.SecurityContext;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static springfox.documentation.builders.PathSelectors.any;
import static springfox.documentation.builders.RequestHandlerSelectors.basePackage;


@Configuration
@EnableSwagger2
public class SwaggerConfig {

    @Value("${spring.security.oauth2.resourceserver.jwt.issuer-uri}")
    private String maskinportenUri;

    @Bean
    public Docket apiDocumentation() {
        return new Docket(DocumentationType.SWAGGER_2)
                .groupName("statistikk-inndata")
                .directModelSubstitute(ZonedDateTime.class, Date.class)
                .enableUrlTemplating(true)
                .select()
                .apis(basePackage(IngestRestController.class.getPackage().getName()))
                .paths(any())
                .paths(Predicates.not(PathSelectors.regex("/version")))
                .paths(Predicates.not(PathSelectors.regex("/health")))
                .build()
                .securitySchemes(singletonList(oauth()))
                .securityContexts(singletonList(securityContext()))
                .apiInfo(apiInfo()
                );
    }

    private ApiInfo apiInfo() {
        final String apiVersion = System.getProperty("difi.version", "N/A");
        return new ApiInfoBuilder()
                .title("Statistikk for offentlige tjenester")
                .description(
                        format(
                                "Beskrivelse av API for innlegging av data (versjon %s).",
                                apiVersion
                        )
                ).version(apiVersion)
                .build();
    }


    // TODO change to jwt-bearer-token instead of basic auth of client
    private List<GrantType> grantTypes() {
        List<GrantType> grantTypes = new ArrayList<>();
        TokenRequestEndpoint tokenRequestEndpoint = new TokenRequestEndpoint(maskinportenUri + "/authorize", "test", "test2");
        TokenEndpoint tokenEndpoint = new TokenEndpoint(maskinportenUri + "/token", "token");
        grantTypes.add(new AuthorizationCodeGrant(tokenRequestEndpoint, tokenEndpoint));
        return grantTypes;
    }

    private SecurityScheme oauth() {
        return new OAuthBuilder()
                .name("OAuth2")
                .scopes(scopes())
                .grantTypes(grantTypes())
                .build();
    }

    private List<AuthorizationScope> scopes() {
        List<AuthorizationScope> list = new ArrayList<>();
        list.add(new AuthorizationScope("digdir:statistikk.skriv", "Skrivetilgong til Statistikk-api for innlegging av tidserier"));
        return list;
    }

    private SecurityContext securityContext() {
        return SecurityContext.builder()
                .securityReferences(Arrays.asList(
                        new SecurityReference("OAuth2", new AuthorizationScope[]{})
                ))
                .forPaths(PathSelectors.ant("/{owner}/{seriesName}/{distance}"))
                .build();
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
