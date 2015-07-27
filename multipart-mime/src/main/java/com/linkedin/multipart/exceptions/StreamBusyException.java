package com.linkedin.multipart.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class StreamBusyException extends GeneralMultiPartMIMEStreamException {
  private static final long serialVersionUID = 1L;

  public StreamBusyException(String message) {
    super(message);
  }

}
