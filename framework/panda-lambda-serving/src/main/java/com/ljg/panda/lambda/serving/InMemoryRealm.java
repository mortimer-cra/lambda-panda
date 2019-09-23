package com.ljg.panda.lambda.serving;

import org.apache.catalina.realm.GenericPrincipal;
import org.apache.catalina.realm.RealmBase;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Adapted from Tomcat's {@link org.apache.catalina.realm.MemoryRealm}.
 * This implementation of {@link RealmBase} lets you add users programmatically to an in-memory store.
 */
public final class InMemoryRealm extends RealmBase {

  static final String NAME = "Panda";

  static final String AUTH_ROLE = "panda-user";

  /**
   * The set of valid Principals for this Realm, keyed by user name.
   */
  private final Map<String, GenericPrincipal> principals = new HashMap<>();

  @Override
  public Principal authenticate(String username, String credentials) {
    GenericPrincipal principal = principals.get(username);
    boolean validated = principal != null && getCredentialHandler().matches(credentials, principal.getPassword());
    return validated ? principal : null;
  }

  void addUser(String username, String password) {
    principals.put(username, new GenericPrincipal(username, password, Collections.singletonList(AUTH_ROLE)));
  }

  @Override
  protected String getPassword(String username) {
    GenericPrincipal principal = principals.get(username);
    return principal == null ? null : principal.getPassword();
  }

  @Override
  protected Principal getPrincipal(String username) {
    return principals.get(username);
  }

}
