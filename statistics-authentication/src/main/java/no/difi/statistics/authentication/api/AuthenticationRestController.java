package no.difi.statistics.authentication.api;

import no.difi.statistics.authentication.AuthenticationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;

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

    @PostMapping("/authentications")
    public AuthenticationResponse authenticate(@RequestBody AuthenticationRequest request) {
        return AuthenticationResponse.builder().authenticated(service.authenticate(request.getUsername(), request.getPassword())).build();
    }

}
