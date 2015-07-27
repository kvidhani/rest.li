package com.linkedin.multipart;

import com.linkedin.data.ByteString;

/**
 * @author Karim Vidhani
 *
 * Used to register with {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader} to
 * asynchronously drive through the reading of a single part.
 *
 * Most implementations of this should pass along a reference to the {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader}
 * during construction. This way when they are invoked on {@link SinglePartMIMEReaderCallback#onPartDataAvailable(com.linkedin.data.ByteString)},
 * they can then turn around and call {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader#requestPartData()}.
 */
public interface SinglePartMIMEReaderCallback {
    /**
     * Invoked when data is available to be read on the current part.
     *
     * @param partData the dat
     */
    public void onPartDataAvailable(ByteString partData);

    /**
     * Invoked when the current part is finished being read.
     */
    public void onFinished();

    /**
     * Invoked when the current part is finished being abandoned.
     */
    public void onAbandoned();

    /**
     * Invoked when there was an error reading from the multipart envelope.
     *
     * @param throwable the Throwable that caused this to happen.
     */
    public void onStreamError(Throwable throwable);
}