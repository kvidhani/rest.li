package com.linkedin.multipart.exceptions;

/**
 * @author Karim Vidhani
 *
 * Represents a general exception when reading from the {@link com.linkedin.multipart.MultiPartMIMEReader}.
 */
public class GeneralMultiPartMIMEReaderStreamException extends RuntimeException
{
  private static final long serialVersionUID = 1L;

  public GeneralMultiPartMIMEReaderStreamException(String message) {
    super(message);
  }
}
