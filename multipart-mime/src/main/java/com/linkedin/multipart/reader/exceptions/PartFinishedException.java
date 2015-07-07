package com.linkedin.multipart.reader.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class PartFinishedException  extends GeneralMimeStreamException {
  private static final long serialVersionUID = 1L;

  public PartFinishedException(String message) {
    super(message);
  }

  public PartFinishedException() {
    super();
  }
}
