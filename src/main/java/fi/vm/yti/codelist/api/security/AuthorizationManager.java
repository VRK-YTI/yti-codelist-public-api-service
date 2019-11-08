package fi.vm.yti.codelist.api.security;

import java.util.UUID;

import fi.vm.yti.security.YtiUser;

public interface AuthorizationManager {

    boolean isSuperUser();

    UUID getUserId();

    YtiUser getUser();
}
