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
   * @throws java.lang.IllegalStateException if the handle has already been closed
   */
  public void write(final ByteString data);

  /**
   * Signals that data source has finished. Any remaining data to finish off the data source
   * should be passed here indicating the source is complete.
   * Data sources should close the appropriate data sources before calling done() on DataSourceHandle
   * @throws java.lang.IllegalStateException if the handle has already been closed
   */
  public void done(final ByteString remainingData);

  /**
   * Signals that the data source has encountered an error.
   *
   * @param throwable the cause of the error.
   */
  public void error(final Throwable throwable);

  enum HandleState
  {
    ACTIVE, //Eligible to write bytes out
    CLOSED //This data source has called done(), or it has called error()
  }
}
