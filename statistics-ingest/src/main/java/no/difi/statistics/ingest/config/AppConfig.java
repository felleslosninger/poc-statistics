package no.difi.statistics.ingest.config;

import no.difi.statistics.ingest.api.IngestRestController;
import no.difi.statistics.ingest.poc.DifiAdminIngester;
import no.difi.statistics.ingest.poc.RandomIngesterRestController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

@Configuration
@EnableAutoConfiguration
@EnableWebSecurity
public class AppConfig extends WebSecurityConfigurerAdapter {

    @Autowired
    private BackendConfig backendConfig;

    @Bean
    public IngestRestController api() {
        return new IngestRestController(backendConfig.ingestService());
    }

    @Bean
    public RandomIngesterRestController randomApi() {
        return new RandomIngesterRestController(backendConfig.ingestService());
    }

    @Bean
    public AuthenticationProvider authenticationProvider() {
        DaoAuthenticationProvider provider = new DaoAuthenticationProvider();
        provider.setUserDetailsService(backendConfig.userDetailsService());
        provider.setPasswordEncoder(new BCryptPasswordEncoder());
        return provider;
    }

    @Bean
    public DifiAdminIngester difiAdminIngester() {
        return new DifiAdminIngester(backendConfig.ingestService());
    }

    @Override
    public void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.authenticationProvider(authenticationProvider());
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.authorizeRequests()
                .antMatchers("/").permitAll()
                .antMatchers("/{owner}/{seriesName}/minute/**").access("#owner == authentication.name")
                .antMatchers("/{owner}/{seriesName}/minutes/**").access("#owner == authentication.name")
                .anyRequest().authenticated()
                .and()
                .httpBasic()
                .and()
                .csrf().disable();
    }

}
