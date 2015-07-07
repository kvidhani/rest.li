package com.linkedin.multipart.reader.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class PartNotInitializedException  extends GeneralMimeStreamException {
  private static final long serialVersionUID = 1L;

  public PartNotInitializedException(String message) {
    super(message);
  }

  public PartNotInitializedException() {
    super();
  }
}
