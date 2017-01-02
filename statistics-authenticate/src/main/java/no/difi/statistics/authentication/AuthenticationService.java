package no.difi.statistics.authentication;

import org.elasticsearch.client.Client;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang3.RandomStringUtils.random;

public class AuthenticationService {

    private static final char[] validPasswordCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789~`!@#$%^&*()-_=+[{]}\\|;:,<.>/?".toCharArray();
    private AuthenticationProvider authenticationProvider;
    private Client client;

    public AuthenticationService(AuthenticationProvider authenticationProvider, Client client) {
        this.authenticationProvider = authenticationProvider;
        this.client = client;
    }

    public void authenticate(String username, String password) throws BadCredentialsException {
        UsernamePasswordAuthenticationToken request = new UsernamePasswordAuthenticationToken(username, password);
        authenticationProvider.authenticate(request).isAuthenticated();
    }

    public String createCredentials(String username) {
        final int passwordLength = 20;
        String password = random(passwordLength, 0, 0, false, false, validPasswordCharacters, new SecureRandom());
        client.prepareIndex("authentication", "authentication", username)
                .setSource(document(username, password))
                .get();
        if (password.length() != passwordLength)
            throw new IllegalStateException(
                    format(
                            "Password length %d of generated password is not equal to expected length %d",
                            password.length(),
                            passwordLength
                    )
            );
        return password;
    }

    private Map<String, String> document(String username, String password) {
        Map<String, String> document = new HashMap<>();
        document.put("username", username);
        document.put("password", new BCryptPasswordEncoder().encode(password));
        return document;
    }

}
