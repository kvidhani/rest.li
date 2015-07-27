package com.linkedin.multipart.exceptions;

/**
 * @author Karim Vidhani
 *
 * Represents an error when trying to use APIs on {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader}
 * or {@link com.linkedin.multipart.MultiPartMIMEReader} in an incorrect state.
 */
public class StreamBusyException extends GeneralMultiPartMIMEReaderStreamException {
  private static final long serialVersionUID = 1L;

  public StreamBusyException(String message) {
    super(message);
  }
}
