package no.difi.statistics.authentication;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.singletonList;

public class ElasticsearchUserDetailsService implements UserDetailsService {

    private RestHighLevelClient client;

    public ElasticsearchUserDetailsService(RestHighLevelClient client) {
        this.client = client;
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        try {
            return user(client.get(new GetRequest("authentication", "authentication", username), RequestOptions.DEFAULT).getSource());
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.NOT_FOUND)
                throw new UsernameNotFoundException(username);
            throw new RuntimeException("Failed to load user by username", e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load user by username", e);
        }
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
