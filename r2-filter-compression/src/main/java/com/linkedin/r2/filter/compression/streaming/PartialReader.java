package com.linkedin.r2.filter.compression.streaming;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import java.util.LinkedList;
import java.util.Queue;


/**
 * Reads at least specified number of bytes from a {@link com.linkedin.r2.message.streaming.EntityStream}.
 *
 * @author Ang Xu
 */
public class PartialReader implements Reader
{
  private final int _numBytes;
  private final Callback<EntityStream[]> _callback;

  private final Queue<ByteString> _buffer = new LinkedList<ByteString>();
  private ReadHandle _rh;
  private WriteHandle _remainingWh;
  private int _readLen;
  private int _outstanding;


  public PartialReader(int numBytes, Callback<EntityStream[]> callback)
  {
    _numBytes = numBytes;
    _callback = callback;
    _readLen = 0;
  }

  @Override
  public void onInit(ReadHandle rh)
  {
    _rh = rh;
    _rh.request(1);
  }

  @Override
  public void onDataAvailable(ByteString data)
  {
    if (_remainingWh == null)
    {
      _buffer.add(data);
      _readLen += data.length();

      if (_readLen <= _numBytes)
      {
        _rh.request(1);
      }
      else
      {
        EntityStream stream = EntityStreams.newEntityStream(new ByteStringsWriter(_buffer));
        EntityStream remaining = EntityStreams.newEntityStream(new RemainingWriter());
        _callback.onSuccess(new EntityStream[] {stream, remaining});
      }
    }
    else
    {
      _outstanding--;
      _remainingWh.write(data);
      int diff = _remainingWh.remaining() - _outstanding;
      if (diff > 0)
      {
        _rh.request(diff);
        _outstanding += diff;
      }
    }
  }

  @Override
  public void onDone()
  {
    if (_remainingWh == null)
    {
      EntityStream stream = EntityStreams.newEntityStream(new ByteStringsWriter(_buffer));
      _callback.onSuccess(new EntityStream[] {stream});
    }
    else
    {
      _remainingWh.done();
    }
  }

  @Override
  public void onError(Throwable e)
  {
    if (_remainingWh == null)
    {
      _callback.onError(e);
    }
    else
    {
      _remainingWh.error(e);
    }
  }

  private class RemainingWriter implements Writer
  {
    @Override
    public void onInit(WriteHandle wh)
    {
      _remainingWh = wh;
    }

    @Override
    public void onWritePossible()
    {
      _outstanding = _remainingWh.remaining();
      _rh.request(_outstanding);
    }

    @Override
    public void onAbort(Throwable e)
    {

    }
  }
}
