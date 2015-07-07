package com.linkedin.multipart.reader.exceptions;

/**
 * Created by kvidhani on 6/11/15.
 */
public class IllegalMimeFormatException extends GeneralMimeStreamException {
    private static final long serialVersionUID = 1L;

    public IllegalMimeFormatException(String message)
    {
        super(message);
    }

}
