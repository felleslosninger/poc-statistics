package no.difi.statistics.authentication.api;

import no.difi.statistics.authentication.AuthenticationService;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;

import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.NO_CONTENT;

@RestController
public class AuthenticationRestController {

    private AuthenticationService service;

    public AuthenticationRestController(AuthenticationService service) {
        this.service = service;
    }

    @GetMapping("/")
    public RedirectView index() throws IOException {
        return new RedirectView("swagger-ui.html");
    }

    @ExceptionHandler
    @ResponseStatus(NO_CONTENT)
    public void badCredentials(BadCredentialsException e) {
    }

    @PostMapping("/authentications")
    @ResponseStatus(CREATED)
    public AuthenticationResponse authenticate(@RequestBody AuthenticationRequest request) {
        service.authenticate(request.getUsername(), request.getPassword());
        return AuthenticationResponse.builder().authenticated(true).build();
    }

    @PostMapping("/credentials/{username}")
    @ResponseStatus(CREATED)
    public CredentialsResponse createCredentials(@PathVariable String username) {
        return new CredentialsResponse(service.createCredentials(username));
    }

    @PostMapping("/credentials/{username}/short")
    @ResponseStatus(CREATED)
    public String createCredentialsShort(@PathVariable String username) {
        return service.createCredentials(username);
    }

}
