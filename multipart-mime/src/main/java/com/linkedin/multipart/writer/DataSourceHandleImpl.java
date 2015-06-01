package com.linkedin.multipart.writer;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;


/**
 * Created by kvidhani on 5/19/15.
 */
//todo update the interface to mention these new exceptions
//todo update this for thread safety...perhaps with atomic references?
//todo redo javadocs

public class DataSourceHandleImpl implements DataSourceHandle
{
  private final WriteHandle _writeHandle;
  //todo how to make this configurable
  private HandleState _state;

  //Package private construction
  DataSourceHandleImpl(final WriteHandle writeHandle)
  {
    _writeHandle = writeHandle;
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
  public void done(ByteString remainingData) throws IllegalStateException, IllegalArgumentException
  {
    //todo consider moving validation to a single method
    if(_state == HandleState.CLOSED)
    {
      throw new IllegalStateException("This DataSourceHandle is closed");
    }

    _state = HandleState.CLOSED; //TODO use atomic reference in case multiple threads deal with this?
    _writeHandle.write(remainingData);
  }

  @Override
  public void error(Throwable throwable)  throws IllegalStateException
  {
    //todo - ask Zhenkai if its safe to do this over and over
    _state = HandleState.CLOSED;
    _writeHandle.error(throwable);
  }

}