package com.linkedin.multipart.exceptions;

/**
 * @author Karim Vidhani
 *
 * Represents an error when trying to re-register a callback with a
 * {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader}.
 */
public class PartBindException  extends GeneralMultiPartMIMEReaderStreamException {
  private static final long serialVersionUID = 1L;

  public PartBindException(String message) {
    super(message);
  }
}
