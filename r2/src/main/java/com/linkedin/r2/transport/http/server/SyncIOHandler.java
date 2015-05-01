package com.linkedin.r2.transport.http.server;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This writer deals with Synchronous IO, which is the case for Servlet API 3.0 & Jetty 8
 *
 * This Writer reads from ServletInputStream and writes to the EntityStream of a request; and reads from
 * the EntityStream of a response and writes into ServletOutputStream.
 *
 * @author Zhenkai Zhu
 */
public class SyncIOHandler implements Writer, Reader
{
  final private ServletInputStream _is;
  final private ServletOutputStream _os;
  final private int _maxBufferedChunks;
  final private BlockingQueue<Event> _eventQueue;
  private WriteHandle _wh;
  private ReadHandle _rh;
  private boolean _requestReadFinished;
  private boolean _responseWriteFinished;

  public SyncIOHandler(ServletInputStream is, ServletOutputStream os, int maxBufferedChunks)
  {
    _is = is;
    _os = os;
    _maxBufferedChunks = maxBufferedChunks;
    _eventQueue = new LinkedBlockingDeque<Event>();
    _requestReadFinished = false;
    _responseWriteFinished = false;
  }

  @Override
  public void onInit(WriteHandle wh)
  {
    _wh = wh;
  }

  @Override
  public void onWritePossible()
  {
    _eventQueue.add(Event.WriteRequestPossibleEvent);
  }

  @Override
  public void onInit(ReadHandle rh)
  {
    _rh = rh;
    _rh.request(_maxBufferedChunks);
  }

  @Override
  public void onDataAvailable(ByteString data)
  {
    _eventQueue.add(new Event(EventType.ResponseDataAvailable, data));
  }

  @Override
  public void onDone()
  {
    _eventQueue.add(Event.FullResponseReceivedEvent);
  }

  @Override
  public void onError(Throwable e)
  {
    _eventQueue.add(new Event(EventType.ResponseDataError, e));
  }

  public void loop() throws ServletException, IOException
  {
    // TODO [ZZ]: consider adding timeout here, e.g. if no activity in 30 seconds (configurable)
    while(shouldContinue())
    {

      Event event;
      try
      {
        event = _eventQueue.take();
      }
      catch (Exception ex)
      {
        throw new ServletException(ex);
      }

      switch (event.getEventType())
      {
        case ResponseDataAvailable:
        {
          ByteString data =  (ByteString) event.getData();
          data.write(_os);
          _rh.request(1);
          break;
        }
        case WriteRequestPossible:
        {
          byte[] buf = new byte[4096];
          while (_wh.remaining() > 0)
          {
            int actualLen = _is.read(buf);

            if (actualLen < 0)
            {
              _wh.done();
              _requestReadFinished = true;
              break;
            }
            _wh.write(ByteString.copy(buf, 0, actualLen));
          }
          break;
        }
        case FullResponseReceived:
        {
          _os.close();
          _responseWriteFinished = true;
          break;
        }
        case ResponseDataError:
        {
          throw new ServletException((Throwable)event.getData());
        }
        default:
          throw new IllegalStateException("Unknown event type:" + event.getEventType());
      }
    }
  }

  protected boolean shouldContinue()
  {
    return !_responseWriteFinished || !_requestReadFinished;
  }

  protected boolean responseWriteFinished()
  {
    return _responseWriteFinished;
  }

  protected boolean requestReadFinished()
  {
    return _requestReadFinished;
  }

  private static enum  EventType
  {
    WriteRequestPossible,
    FullResponseReceived,
    ResponseDataAvailable,
    ResponseDataError,
  }

  private static class Event
  {
    private final EventType _eventType;
    private final Object _data;

    static Event WriteRequestPossibleEvent = new Event(EventType.WriteRequestPossible);
    static Event FullResponseReceivedEvent = new Event(EventType.FullResponseReceived);

    Event(EventType eventType)
    {
      this(eventType, null);
    }

    Event(EventType eventType, Object data)
    {
      _eventType = eventType;
      _data = data;
    }

    EventType getEventType()
    {
      return _eventType;
    }

    Object getData()
    {
      return _data;
    }
  }
}
