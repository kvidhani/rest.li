package com.linkedin.multipart.reader.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class ReaderNotInitializedException extends GeneralMimeStreamException {
  private static final long serialVersionUID = 1L;

  public ReaderNotInitializedException(String message) {
    super(message);
  }

  public ReaderNotInitializedException() {
    super();
  }
}
