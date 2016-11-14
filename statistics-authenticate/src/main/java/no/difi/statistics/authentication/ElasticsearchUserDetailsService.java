package no.difi.statistics.authentication;

import org.elasticsearch.client.Client;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import java.util.Map;

import static java.util.Collections.singletonList;

public class ElasticsearchUserDetailsService implements UserDetailsService {

    private Client client;

    public ElasticsearchUserDetailsService(Client client) {
        this.client = client;
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        return user(client.prepareGet("authentication", "authentication", username).get().getSource());
    }

    private UserDetails user(Map<String, Object> document) {
        return new User(username(document), password(document), singletonList(new SimpleGrantedAuthority("USER")));
    }

    private String username(Map<String, Object> document) {
        return string("username", document);
    }

    private String password(Map<String, Object> document) {
        return string("password", document);
    }

    private String string(String key, Map<String, Object> document) {
        return (String)document.get(key);
    }

}
