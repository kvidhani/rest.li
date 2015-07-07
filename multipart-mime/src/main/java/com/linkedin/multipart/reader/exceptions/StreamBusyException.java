package com.linkedin.multipart.reader.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class StreamBusyException extends GeneralMimeStreamException {
  private static final long serialVersionUID = 1L;

  public StreamBusyException(String message) {
    super(message);
  }

  public StreamBusyException() {
    super();
  }
}
