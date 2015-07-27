package com.linkedin.multipart.exceptions;

/**
 * Created by kvidhani on 6/5/15.
 */
public class ReaderFinishedException extends GeneralMultiPartMIMEReaderStreamException {
  private static final long serialVersionUID = 1L;

  public ReaderFinishedException(String message) {
    super(message);
  }

}
