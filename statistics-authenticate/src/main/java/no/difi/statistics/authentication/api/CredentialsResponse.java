package no.difi.statistics.authentication.api;

public class CredentialsResponse {

    private String password;

    public CredentialsResponse() {
    }

    public CredentialsResponse(String password) {
        this.password = password;
    }

    public String getPassword() {
        return password;
    }
}
