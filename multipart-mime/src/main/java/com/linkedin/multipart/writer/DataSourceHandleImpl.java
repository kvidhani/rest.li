package com.linkedin.multipart.writer;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;


/**
 * Created by kvidhani on 5/19/15.
 */
//todo update this for thread safety...perhaps with atomic references?
//todo redo javadocs

public class DataSourceHandleImpl implements DataSourceHandle
{
  private final WriteHandle _writeHandle;
  private final MultiPartMIMEWriter _writer;
  private HandleState _state;

  //Package private construction
  DataSourceHandleImpl(final WriteHandle writeHandle, final MultiPartMIMEWriter writer)
  {
    _writeHandle = writeHandle;
    _writer = writer;
    _state = HandleState.ACTIVE;
  }

  @Override
  public void write(final ByteString data) throws IllegalStateException, IllegalArgumentException
  {
    if(_state == HandleState.CLOSED)
    {
      throw new IllegalStateException("This DataSourceHandle is closed");
    }

    _writeHandle.write(data);
  }

  @Override
  public void done(ByteString remainingData) throws IllegalStateException
  {
    if(_state == HandleState.CLOSED)
    {
      throw new IllegalStateException("This DataSourceHandle is closed");
    }

    _state = HandleState.CLOSED;
    //We first transition the data source. Otherwise if we write first, then a new call to onWritePossible() could
    //occur in the writer inside of MultiPartMIMEWriter before our transition finishes.
    _writer.currentDataSourceFinished();
    _writeHandle.write(remainingData);
  }

  @Override
  public void error(Throwable throwable)
  {
    _state = HandleState.CLOSED;
    _writeHandle.error(throwable);
    _writer.abortAllDataSources(throwable); //If this data source has encountered an error
    //everyone else in front of them needs to know
  }
}