package com.linkedin.multipart.writer;

import com.linkedin.data.ByteString;


/**
 * Created by kvidhani on 5/18/15.
 */
//todo redo javadocs
public interface DataSourceHandle {


  /**
   * This writes data out from the data source.
   *
   * @param data the data to be written
   * @throws java.lang.IllegalStateException
   * had been previously told to abort.
   * @throws java.lang.IllegalArgumentException
   */
  void write(final ByteString data) throws IllegalStateException, IllegalArgumentException;

  /**
   * Signals that data source has finished. Any remaining data to finish off the data source
   * should be passed here indicating the source is complete. The size of this ByteString must be greater then
   * or equal to 0.
   * @throws java.lang.IllegalStateException if done() or error() has been called or if this data source
   * had been previously told to abort.
   * @throws java.lang.IllegalArgumentException
   */
  void done(final ByteString remainingData);

  /**
   * Signals that the data source has encountered an error.
   *
   * @param throwable the cause of the error.
   */
  void error(final Throwable throwable);

  enum HandleState
  {
    ACTIVE, //Eligible to write bytes out
    CLOSED //This data source has called done(), or it has called error(),
    //or it has been aborted (along with a call to the MultiPartMIMEDataSource
  }
}
