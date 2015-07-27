package com.linkedin.multipart.exceptions;

/**
 * Created by kvidhani on 6/11/15.
 */
public class IllegalMultiPartMIMEFormatException extends GeneralMultiPartMIMEReaderStreamException {
    private static final long serialVersionUID = 1L;

    public IllegalMultiPartMIMEFormatException(String message)
    {
        super(message);
    }

}
