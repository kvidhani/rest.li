package com.linkedin.multipart.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class StreamFinishedException extends GeneralMultiPartMIMEStreamException {
  private static final long serialVersionUID = 1L;

  public StreamFinishedException(String message) {
    super(message);
  }

}
