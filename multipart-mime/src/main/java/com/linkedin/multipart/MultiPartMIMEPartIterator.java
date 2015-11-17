package com.linkedin.multipart;


/**
 * Interface used by {@link com.linkedin.multipart.MultiPartMIMEReader} to register with and begin the reading of
 * parts within a multipart/mime envelope.
 *
 * @author Karim Vidhani
 */
public interface MultiPartMIMEPartIterator
{
  /**
   * Indicates if all parts have been finished and completely read from this MultiPartMIMEReader. If the last part is
   * in the process of being read, this will return false.
   *
   * @return true if the reader is completely finished.
   */
  public boolean haveAllPartsFinished();

  /**
   * Reads through and abandons the current new part (if applicable) and additionally the whole stream.
   *
   * This API can be used in only the following scenarios:
   *
   * 1. Without a registration using a {@link com.linkedin.multipart.MultiPartMIMEReaderCallback}. Abandonment will begin
   * and since no callback is registered, there will be no notification when it is completed.
   *
   * 2. After registration using a {@link com.linkedin.multipart.MultiPartMIMEReaderCallback}
   * and after an invocation on {@link MultiPartMIMEReaderCallback#onNewPart(com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader)}.
   * Abandonment will begin and when it is complete, a call will be made to {@link MultiPartMIMEReaderCallback#onAbandoned()}.
   *
   * If this is called after registration and before an invocation on
   * {@link MultiPartMIMEReaderCallback#onNewPart(com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader)},
   * then a {@link com.linkedin.multipart.exceptions.StreamBusyException} will be thrown.
   *
   * If this used after registration, then this can ONLY be called if there is no part being actively read, meaning that
   * the current {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader} has not been initialized
   * with a {@link com.linkedin.multipart.SinglePartMIMEReaderCallback}. If this is violated a
   * {@link com.linkedin.multipart.exceptions.StreamBusyException} will be thrown.
   *
   * If the stream is finished, subsequent calls will throw {@link com.linkedin.multipart.exceptions.MultiPartReaderFinishedException}.
   *
   * Since this is async and request queueing is not allowed, repetitive calls will result in
   * {@link com.linkedin.multipart.exceptions.StreamBusyException}.
   */
  public void abandonAllParts();

  /**
   * Register to read using this MultiPartMIMEReader. This can ONLY be called if there is no part being actively
   * read; meaning that the current {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader}
   * has not had a callback registered with it. Violation of this will throw a {@link com.linkedin.multipart.exceptions.StreamBusyException}.
   *
   * This can even be set if no parts in the stream have actually been consumed, i.e after the very first invocation of
   * {@link MultiPartMIMEReaderCallback#onNewPart(com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader)}.
   *
   * If this MultiPartMIMEReader is finished, then attempts to register a callback will throw
   * {@link com.linkedin.multipart.exceptions.MultiPartReaderFinishedException}.
   *
   * @param clientCallback the {@link com.linkedin.multipart.MultiPartMIMEReaderCallback} which will be invoked upon
   *                       to read this multipart mime body.
   */
  public void registerReaderCallback(final MultiPartMIMEReaderCallback clientCallback);

}
