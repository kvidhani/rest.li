package com.linkedin.multipart.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class PartNotInitializedException  extends GeneralMultiPartMIMEStreamException {
  private static final long serialVersionUID = 1L;

  public PartNotInitializedException(String message) {
    super(message);
  }

}
