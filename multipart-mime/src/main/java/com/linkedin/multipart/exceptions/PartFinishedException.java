package com.linkedin.multipart.exceptions;

/**
 * @author Karim Vidhani
 *
 * Represents an error when trying to use a already finished
 * {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader}.
 */
public class PartFinishedException extends GeneralMultiPartMIMEReaderStreamException {
  private static final long serialVersionUID = 1L;

  public PartFinishedException(String message) {
    super(message);
  }
}
