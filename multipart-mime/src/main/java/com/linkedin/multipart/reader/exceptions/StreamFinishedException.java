package com.linkedin.multipart.reader.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class StreamFinishedException extends GeneralMimeStreamException {
  private static final long serialVersionUID = 1L;

  public StreamFinishedException(String message) {
    super(message);
  }

  public StreamFinishedException() {
    super();
  }
}
