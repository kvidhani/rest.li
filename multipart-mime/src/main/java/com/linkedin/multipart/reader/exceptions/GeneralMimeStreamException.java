package com.linkedin.multipart.reader.exceptions;

/**
 * Created by kvidhani on 6/11/15.
 */
//todo all stream exceptions should extend from this
public class GeneralMimeStreamException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public GeneralMimeStreamException(String message) {
    super(message);
  }

  public GeneralMimeStreamException() {
    super();
  }
}
