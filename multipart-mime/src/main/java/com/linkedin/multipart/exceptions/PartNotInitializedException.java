package com.linkedin.multipart.exceptions;

/**
 * @author Karim Vidhani
 *
 * Represents an error when trying to use APIs on {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader}
 * without prior callback registration.
 */
public class PartNotInitializedException  extends GeneralMultiPartMIMEReaderStreamException {
  private static final long serialVersionUID = 1L;

  public PartNotInitializedException(String message) {
    super(message);
  }
}
