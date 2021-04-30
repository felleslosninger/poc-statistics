package no.difi.statistics.ingest.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class SecurityConfig extends WebSecurityConfigurerAdapter implements WebMvcConfigurer {


    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.authorizeRequests()
                // No authentication required for documentation paths used by Swagger
                .antMatchers(GET, "/", "/swagger-ui.html", "/swagger-resources/**", "/v2/api-docs/**", "/webjars/**").permitAll()
                // No authentication required for health check path or env
                .antMatchers(GET, "/health", "/env/**").permitAll()
                // Authentication required for ingest methods. Username must be equal to owner of series.
                .antMatchers(POST, "/{owner}/{seriesName}/**").authenticated()
                // No authentication required for getting last point on a series
                .antMatchers(GET, "/{owner}/{seriesName}/{distance}/last").permitAll()
                .anyRequest().authenticated()
                .and()
                .oauth2ResourceServer()
                .jwt();
    }

    @Override
    public void configure(WebSecurity web) {
        web.ignoring().antMatchers("/v2/api-docs",
                "/configuration/ui",
                "/swagger-resources/**",
                "/configuration/security",
                "/swagger-ui.html",
                "/webjars/**");
    }
}
