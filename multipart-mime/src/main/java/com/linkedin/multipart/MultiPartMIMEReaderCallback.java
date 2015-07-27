package com.linkedin.multipart;

/**
 * @author Karim Vidhani
 *
 * Used to register with {@link com.linkedin.multipart.MultiPartMIMEReader} to asynchronously
 * drive through the reading of a multipart mime envelope.
 */
public interface MultiPartMIMEReaderCallback {
    /**
     * Invoked (at some time in the future) upon a registration with a {@link com.linkedin.multipart.MultiPartMIMEReader}.
     * Also invoked when previous parts are finished and new parts are available.
     *
     * @param singleParMIMEReader the SinglePartMIMEReader which can be used to walk through this part.
     */
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader);

    /**
     * Invoked when this reader is finished and the multipart mime envelope has been completely read.
     */
    public void onFinished();

    /**
     * Invoked as a result of calling {@link MultiPartMIMEReader#abandonAllParts()}. This will be invoked
     * at some time in the future when all the parts from this multipart mime envelope are abandoned.
     */
    public void onAbandoned();

    /**
     * Invoked when there was an error reading from the multipart envelope.
     *
     * @param throwable the Throwable that caused this to happen.
     */
    public void onStreamError(Throwable throwable);
}