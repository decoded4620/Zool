package com.decoded.zool;

/**
 * For Zool Specific Exception Class.
 */
public class ZoolServiceException extends RuntimeException {
  public ZoolServiceException(String message) {
    super(message);
  }

  public ZoolServiceException(String message, Throwable cause) {
    super(message, cause);
  }

  public ZoolServiceException(Throwable cause) {
    super(cause);
  }

  public ZoolServiceException() {
    super();
  }
}
