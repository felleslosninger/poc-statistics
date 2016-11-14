package no.difi.statistics.authentication;

import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;

public class AuthenticationService {

    private AuthenticationProvider authenticationProvider;

    public AuthenticationService(AuthenticationProvider authenticationProvider) {
        this.authenticationProvider = authenticationProvider;
    }

    public boolean authenticate(String username, String password) {
        UsernamePasswordAuthenticationToken request = new UsernamePasswordAuthenticationToken(username, password);
        try {
            return authenticationProvider.authenticate(request).isAuthenticated();
        } catch (BadCredentialsException e) {
            return false;
        }
    }

}
