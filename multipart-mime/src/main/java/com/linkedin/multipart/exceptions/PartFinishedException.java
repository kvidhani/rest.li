package com.linkedin.multipart.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class PartFinishedException extends GeneralMultiPartMIMEStreamException {
  private static final long serialVersionUID = 1L;

  public PartFinishedException(String message) {
    super(message);
  }

}
