package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Created by kvidhani on 5/18/15.
 */
//Todo - Javadocs
//Mention that this class closes the underlying input stream when either of the following happen:
//1. The stream is finished being read
//2. There was an exception reading the stream, so we then close the stream and call error on write handle
//3. The write was aborted, in which case we also close the stream.

  //todo use a delegate pattern to forbid external users from calling interface methods directly
  //todo what needs to be volatile?
public final class MultiPartMIMEInputStream implements MultiPartMIMEDataSource
{
  public static final int DEFAULT_MAXIMUM_BLOCKING_DURATION = 3000;
  public static final int DEFAULT_WRITE_CHUNK_SIZE = 5000;

  private volatile WriteHandle _writeHandle;
  private final Map<String, String> _headers;
  private final InputStream _inputStream;
  private boolean _dataSourceFinished = false; //since there is no way to see if an InputStream has already been closed
  private final ExecutorService _executorService;
  private final int _maximumBlockingTime;
  private final int _writeChunkSize;

  @Override
  public void onInit(final WriteHandle writeHandle)
  {
    _writeHandle = writeHandle;
  }

  @Override
  public void onWritePossible() {

    //We can't use the traditional while (_writeHandle.remaining() > 0) since we need to incrementally
    //schedule reads. We need to wait for each one to finish before moving onto the next. Moving onto the next
    //means scheduling a runnable which will call onWritePossible() once again simulating what R2 would do.
    if (_writeHandle.remaining() > 0) {
      final CountDownLatch latch = new CountDownLatch(1);

      //We use two threads from the client provided thread pool. We must use this technique
      //because if the read from the input stream hangs, there is no way to recover.
      //Therefore we have a reader thread and a boss thread that waits for the reader to complete.

      //We want to invoke the callback on only one (boss) thread because:
      //1. It makes the threading model more complicated if the reader thread invokes the callbacks as well as
      //the boss thread.
      //2. In cases where there is a timeout, meaning the read took too long, we want to close the
      //input stream on the boss thread to unblock the reader thread pool thread. In such cases it is indeterminate
      //as to what the behavior is when the aforementioned blocked thread wakes up. This thread could receive an IOException
      //or come back with a few bytes. Due to this indeterminate behavior we don't want to trust the reader thread
      //to invoke any callbacks. We want our boss thread to explicitly invoke error() in such cases.
      final InputStreamReader inputStreamReader = new InputStreamReader(latch);
      final InputStreamReaderManager inputStreamReaderManager = new InputStreamReaderManager(inputStreamReader, latch);
      _executorService.submit(inputStreamReader);
      _executorService.submit(inputStreamReaderManager);
    }
  }

  /**
   * Indicates there was a request to abort writing that was not this particular part's fault.
   * @param e the throwable that caused the writing from this data source to abort.
   */
  @Override
  public void onAbort(Throwable e) {
    try {
      _inputStream.close();
    } catch (IOException ioException) {
      //This can safely be swallowed
    }
  }

  @Override
  public Map<String, String> dataSourceHeaders() {
    return _headers;
  }

  private class InputStreamReader implements Runnable {

    private final CountDownLatch _countDownLatch;
    private ByteString _result = null;
    private Throwable _error = null;

    @Override
    public void run() {

      try {
        byte[] bytes = new byte[_writeChunkSize];
        int bytesRead = _inputStream.read(bytes);
        //The number of bytes 'N' here could be the following:
        if (bytesRead == -1) {
          //1. N==-1. This signifies the stream is complete in the case that we coincidentally read to completion on the
          //last read from the InputStream.
          _dataSourceFinished = true;
          _result = ByteString.empty();
        } else if (bytesRead == _writeChunkSize) {
          //2. N==Capacity. This signifies the most common case which is that we read as many bytes as we originally desired.
          _result = ByteString.copy(bytes);
        } else {
          //3. Capacity > N >= 0. This signifies that the input stream is wrapping up and we just got the last few bytes.
          _dataSourceFinished = true;
          _result = ByteString.copy(bytes, 0, bytesRead);
        }
      } catch (IOException ioException) {
        _error = ioException;
      } finally {
        _countDownLatch.countDown();
      }
    }

    private InputStreamReader(final CountDownLatch latch) {
      _countDownLatch = latch;
    }

  }

  private class InputStreamReaderManager implements Runnable {

    private final InputStreamReader _inputStreamReader;
    private final CountDownLatch _countDownLatch;

    private InputStreamReaderManager(final InputStreamReader inputStreamReader, final CountDownLatch countDownLatch) {
      _inputStreamReader = inputStreamReader;
      _countDownLatch = countDownLatch;
    }

    @Override
    public void run() {

      try {
        boolean successful = _countDownLatch.await(_maximumBlockingTime, TimeUnit.MILLISECONDS);
        if (successful) {
          if (_inputStreamReader._result != null) {
            //If the call went through, extract the info and write
            if (_dataSourceFinished) {
              _writeHandle.write(_inputStreamReader._result);
              _writeHandle.done();
              //Close the stream since we won't be invoked again
              try {
                _inputStream.close();
              } catch (IOException ioException) {
                //Safe to swallow
                //An exception thrown when we try to close the InputStream should not really
                //make its way down as an error...
              }
            } else {
              //Just a normal write
              _writeHandle.write(_inputStreamReader._result);
            }
          } else {
            //This means the result is null which implies
            //that a throwable exists. In this case the read on the InputStream threw an IOException
            //First close the InputStream
            try {
              _inputStream.close();
            } catch (IOException ioException) {
              //Safe to swallow
              //An exception thrown when we try to close the InputStream should not really
              //make its way down as an error...
            }
            //Now mark is it an error using the correct throwable
            _writeHandle.error(_inputStreamReader._error);
          }
        } else {
          //there was a timeout
          try {
            //Close the input stream here since we won't be doing any more reading. This should also potentially
            //free up any threads blocked on the read()
            _inputStream.close();
          } catch (IOException ioException) {
            //Safe to swallow
          }
          _writeHandle.error(new TimeoutException("InputStream reading timed out"));
        }
      } catch (InterruptedException exception) {

        //If this thread interrupted, then we have no choice but to abort everything
        try {
          //Close the input stream here since we won't be doing any more reading.
          _inputStream.close();
        } catch (IOException ioException) {
          //Safe to swallow
        }
        _writeHandle.error(exception);
      } finally {

        //Continue writing only if there is more to do. Note that orthodox writers typically have a
        //while (_writeHandle.remaining() > 0) in onWritePossible(). Once _writeHandle.remaining() returns
        //0, then R2 calls onWritePossible() again with a new remaining count.
        //
        //Since we can't follow that technique (since we have to sequentially read jobs from the input stream),
        //we instead iterate sequentially using subsequent invocations to onWritePossible() which uses an
        //if() loop instead.
        //Therefore we continue only if:
        //1. There is more to write on the writeHandle.
        //AND
        //2. We haven't called done yet.
        if (_writeHandle.remaining() > 0 && _dataSourceFinished == false) {
          final ContinueWritingManager continueWritingManager = new ContinueWritingManager();
          _executorService.submit(continueWritingManager);
        }
      }
    }
  }

  private class ContinueWritingManager implements Runnable {
    @Override
    public void run() {
        MultiPartMIMEInputStream.this.onWritePossible();
    }
  }

    /**
   * Create a new instance of a MultiPartMIMEInputStream that wraps the provided InputStream to
   * construct an individual multipart MIME part within the multipart MIME envelope.
   */
  //todo do java docs and mention in java docs that the thread pool MUST have atleast three threads
  public static class Builder {

    private final InputStream _inputStream;
    private final ExecutorService _executorService;
    private final Map<String, String> _headers;
    private int _maximumBlockingTime = DEFAULT_MAXIMUM_BLOCKING_DURATION;
    private int _writeChunkSize = DEFAULT_WRITE_CHUNK_SIZE;

    //These are all required
    public Builder(final InputStream inputStream, final ExecutorService executorService, final Map<String, String> headers) {
      _inputStream = inputStream;
      _executorService = executorService;
      _headers = headers;
    }

    public Builder withMaximumBlockingTime(final int maximumBlockingTime) {
      _maximumBlockingTime = maximumBlockingTime;
      return this;
    }

    public Builder withWriteChunkSize(final int writeChunkSize) {
      _writeChunkSize = writeChunkSize;
      return this;
    }

    public MultiPartMIMEInputStream build()
    {
      return new MultiPartMIMEInputStream(_inputStream, _executorService, _headers, _maximumBlockingTime, _writeChunkSize);
    }
  }

  //Private construction due to the builder
  private MultiPartMIMEInputStream(final InputStream inputStream, final ExecutorService executorService,
                                   final Map<String, String> headers, final int maximumBlockingTime,
                                   final int writeChunkSize)
  {
    _inputStream = inputStream;
    _executorService = executorService;
    _headers = new HashMap<String, String>(headers); //defensive copy
    _maximumBlockingTime = maximumBlockingTime;
    _writeChunkSize = writeChunkSize;
  }
}