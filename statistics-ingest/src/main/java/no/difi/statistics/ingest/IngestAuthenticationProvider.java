package no.difi.statistics.ingest;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.web.client.RestTemplate;

import javax.xml.bind.annotation.XmlRootElement;

import static java.util.Collections.singletonList;

public class IngestAuthenticationProvider implements AuthenticationProvider {

    private RestTemplate restTemplate;
    private String serviceHost;
    private int servicePort;

    public IngestAuthenticationProvider(RestTemplate restTemplate, String serviceHost, int servicePort) {
        this.restTemplate = restTemplate;
        this.serviceHost = serviceHost;
        this.servicePort = servicePort;
    }

    public Authentication authenticate(Authentication authentication)
            throws AuthenticationException {
        String username = authentication.getPrincipal().toString();
        Object credentials = authentication.getCredentials();
        String password = credentials == null ? null : credentials.toString();
        if (!authenticate(requestEntity(username, password)).getBody().isAuthenticated())
            throw new BadCredentialsException(username);
        return new UsernamePasswordAuthenticationToken(username, password, singletonList(new SimpleGrantedAuthority("USER")));
    }

    private ResponseEntity<AuthenticationResponse> authenticate(HttpEntity<AuthenticationRequest> request) {
        return restTemplate.postForEntity(
                "http://{host}:{port}/authentications",
                request,
                AuthenticationResponse.class,
                serviceHost,
                servicePort
        );
    }

    private HttpEntity<AuthenticationRequest> requestEntity(String user, String password) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        return new HttpEntity<>(new AuthenticationRequest(user, password), headers);
    }

    public boolean supports(Class<?> authentication) {
        return (UsernamePasswordAuthenticationToken.class
                .isAssignableFrom(authentication));
    }

    @XmlRootElement
    private static class AuthenticationRequest {

        private String username;
        private String password;

        public AuthenticationRequest() {
        }

        public AuthenticationRequest(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }
    }

    @XmlRootElement
    private static class AuthenticationResponse {

        private boolean authenticated;

        public boolean isAuthenticated() {
            return authenticated;
        }

    }

}
