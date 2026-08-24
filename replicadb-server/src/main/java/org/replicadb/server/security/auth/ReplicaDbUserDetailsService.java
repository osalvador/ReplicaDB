package org.replicadb.server.security.auth;

import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("api")
public class ReplicaDbUserDetailsService implements UserDetailsService {

    private final AppUserRepository repository;

    public ReplicaDbUserDetailsService(AppUserRepository repository) {
        this.repository = repository;
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        return repository.findByUsername(username)
                .map(ReplicaDbUserDetails::new)
                .orElseThrow(() -> new UsernameNotFoundException("User not found"));
    }
}
