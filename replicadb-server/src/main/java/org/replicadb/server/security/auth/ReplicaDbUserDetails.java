package org.replicadb.server.security.auth;

import org.replicadb.server.security.domain.AppUser;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public final class ReplicaDbUserDetails implements UserDetails {

    private final AppUser appUser;

    public ReplicaDbUserDetails(AppUser appUser) {
        this.appUser = appUser;
    }

    public UUID userId() {
        return appUser.id();
    }

    public AppUser appUser() {
        return appUser;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return List.of(new SimpleGrantedAuthority("ROLE_" + appUser.role().name()));
    }

    @Override
    public String getPassword() {
        return appUser.passwordHash();
    }

    @Override
    public String getUsername() {
        return appUser.username();
    }

    @Override
    public boolean isAccountNonExpired() {
        return appUser.enabled();
    }

    @Override
    public boolean isAccountNonLocked() {
        return appUser.enabled();
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return appUser.enabled();
    }

    @Override
    public boolean isEnabled() {
        return appUser.enabled();
    }
}
