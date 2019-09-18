package com.ljg.panda.api.serving;

import javax.ws.rs.core.Response;
import java.util.Objects;

/**
 * Thrown by Serving Layer endpoints to indicate an error in processing.
 *
 */
public final class PandaServingException extends Exception {

  private final Response.Status statusCode;

  /**
   * @param statusCode HTTP status that this exception corresponds to
   */
  public PandaServingException(Response.Status statusCode) {
    this(statusCode, null);
  }

  /**
   * @param statusCode HTTP status that this exception corresponds to
   * @param message additional exception message that's appropriate for HTTP status line
   */
  public PandaServingException(Response.Status statusCode, String message) {
    super(message);
    Objects.requireNonNull(statusCode);
    this.statusCode = statusCode;
  }

  /**
   * @return HTTP status that this exception corresponds to
   */
  public Response.Status getStatusCode() {
    return statusCode;
  }
}