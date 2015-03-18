package com.linkedin.r2.message.streaming;

import com.linkedin.data.ByteString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A class consists exclusively of static methods to deal with EntityStream {@link com.linkedin.r2.message.streaming.EntityStream}
 *
 * @author Zhenkai Zhu
 */
public final class EntityStreams
{
  private EntityStreams() {}

  private final static EntityStream EMPTY_STREAM = newEntityStream(new Writer()
  {
    private WriteHandle _wh;
    @Override
    public void onInit(WriteHandle wh)
    {
      _wh = wh;
    }

    @Override
    public void onWritePossible()
    {
      _wh.done();
    }
  });

  public static EntityStream emptyStream()
  {
    return EMPTY_STREAM;
  }

  /**
   * The method to create a new EntityStream with a writer for the stream
   *
   * @param writer the writer for the stream who would provide the data
   * @return an instance of EntityStream
   */
  public static EntityStream newEntityStream(Writer writer)
  {
    return new EntityStreamImpl(writer);
  }


  private static class EntityStreamImpl implements EntityStream
  {
    final private Writer _writer;
    final private Object _lock;
    private List<Observer> _observers;
    private Reader _reader;
    private boolean _initialized;
    // maintains the allowed capacity which is controlled by reader
    private int _capacity;
    private boolean _notifyWritePossible;

    EntityStreamImpl(Writer writer)
    {
      _writer = writer;
      _lock = new Object();
      _observers = new ArrayList<Observer>();
      _initialized = false;
      _capacity = 0;
      _notifyWritePossible = true;
    }

    public void addObserver(Observer o)
    {
      synchronized (_lock)
      {
        checkInit();
        _observers.add(o);
      }
    }

    public void setReader(Reader r)
    {
      synchronized (_lock)
      {
        checkInit();
        _reader = r;
        _initialized = true;
        _observers = Collections.unmodifiableList(_observers);
      }

      final WriteHandle wh = new WriteHandleImpl();
      _writer.onInit(wh);

      final ReadHandle rh = new ReadHandleImpl();
      _reader.onInit(rh);
    }

    private class WriteHandleImpl implements WriteHandle
    {
      @Override
      public void write(final ByteString data)
      {

        int dataLen = data.length();

        // Writer tries to try when the reader didn't request more data
        synchronized (_lock)
        {
          if (_capacity < dataLen)
          {
            throw new IllegalArgumentException("Data size " + dataLen + " is larger than remaining capacity.");
          }
          _capacity -= dataLen;
        }

        for(Observer observer: _observers)
        {
          observer.onDataAvailable(data);
        }
        _reader.onDataAvailable(data);

      }

      @Override
      public void done()
      {
        for(Observer observer: _observers)
        {
          observer.onDone();
        }
        _reader.onDone();
      }

      @Override
      public void error(final Throwable e)
      {
        for(Observer observer: _observers)
        {
          observer.onError(e);
        }
        _reader.onError(e);
      }

      @Override
      public int remainingCapacity()
      {
        synchronized (_lock)
        {
          if (_capacity == 0)
          {
            _notifyWritePossible = true;
          }
          return _capacity;
        }
      }
    }

    private class ReadHandleImpl implements ReadHandle
    {
      @Override
      public void read(final int chunkNum)
      {
        boolean needNotify = false;
        synchronized (_lock)
        {
          _capacity += chunkNum;

          // notify the writer if needed
          if (_notifyWritePossible)
          {
            needNotify = true;
            _notifyWritePossible = false;
          }
        }

        if (needNotify)
        {
          _writer.onWritePossible();
        }
      }
    }

    private void checkInit()
    {
      if (_initialized)
      {
        throw new IllegalStateException("EntityStream had already been initialized and can no longer accept Observers or Reader");
      }
    }
  }
}
