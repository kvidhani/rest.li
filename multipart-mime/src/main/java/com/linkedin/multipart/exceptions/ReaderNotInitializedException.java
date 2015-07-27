package com.linkedin.multipart.exceptions;

/**
 * @author Karim Vidhani
 *
 * Represents an error when trying to use APIs on {@link com.linkedin.multipart.MultiPartMIMEReader}
 * without prior callback registration.
 */
public class ReaderNotInitializedException extends GeneralMultiPartMIMEReaderStreamException {
  private static final long serialVersionUID = 1L;

  public ReaderNotInitializedException(String message) {
    super(message);
  }
}
