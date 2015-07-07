package com.linkedin.multipart.reader.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class PartBindException  extends GeneralMimeStreamException {
  private static final long serialVersionUID = 1L;

  public PartBindException(String message) {
    super(message);
  }

  public PartBindException() {
    super();
  }
}
