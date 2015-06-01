package com.linkedin.multipart.writer;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

//todo  java docs everywhere

/**
 * Created by kvidhani on 5/18/15.
 */
//todo mention that this class closes the underlying input stream when either of the following happen:
//1. The stream is finished being read
//2. There was an exception reading the stream, so we then close the stream and call error on write handle
//3. The write was aborted, in which case we also close the stream.

public final class MultiPartMIMEInputStream implements MultiPartMIMEDataSource {
  private DataSourceHandle _dataSourceHandle;
  private final Map<String, String> _headers;
  private final InputStream _inputStream;
  private boolean _dataSourceFinished = false; //since there is no way to see if an InputStream has already been closed
  private final ExecutorService _executorService;
  private final long _maximumBlockingTime;
  public static final long DEFAULT_MAXIMUM_BLOCKING_DURATION = 3000l;
  public static final long DEFAULT_CHUNK_SIZE = 5000l;

  @Override
  public void onInit(final DataSourceHandle dataSourceHandle) {
    //todo should we check to see if this was not null? This would be a potential bug...
    _dataSourceHandle = dataSourceHandle;
  }

  //todo mention in java docs that application developers MUST make sure that the input stream that they
  //provided can read as many bytes as requested out, otherwise they risk a premature call to onDone() called
  @Override
  public void onWritePossible() {

    //todo - is it worth checking to see if the input stream is already closed? Shouldn't really happen

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

  /**
   * Indicates there was a request to abort writing that was not this particular part's fault.
   * @param e the throwable that caused the writing from this data source to abort.
   */
  @Override
  public void onAbort(Throwable e) {
    try {
      _inputStream.close();
    } catch (IOException ioException) {
      //todo can this exception be swallowed? Perhaps be logged?
    }
  }

  @Override
  public Map<String, String> dataSourceHeaders() {
    return _headers;
  }

  private class InputStreamReader implements Runnable {

    final CountDownLatch _countDownLatch;
    ByteString _result = null;
    Throwable _error = null;

    @Override
    public void run() {

      try {
        byte[] bytes = new byte[_dataSourceHandle.capacity()];
        int bytesRead = _inputStream.read(bytes);
        //The number of bytes 'N' here could be the following:
        if (bytesRead == -1) {
          //1. N==-1. This signifies the stream is complete in the case that we coincidentally read to completion on the
          //last read from the InputStream.
          _dataSourceFinished = true;
          _result = ByteString.empty();
        } else if (bytesRead == _dataSourceHandle.capacity()) {
          //2. N==Capacity. This signifies the most common case which is that we read as many bytes as we were told to read.
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

    private ByteString getResult() {
      return _result;
    }

    private Throwable getError() {
      return _error;
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
              _dataSourceHandle.done(_inputStreamReader._result);
              //Close the stream and shutdown the engine, since we won't be invoked again
              try {
                _inputStream.close();
              } catch (IOException ioException) {
                //todo can this exception be swallowed? Perhaps be logged?
                //An exception thrown when we try to close the InputStream should not really
                //make its way down as an error...
              }
            } else {
              //Just a normal write
              _dataSourceHandle.write(_inputStreamReader._result);
            }
          } else {
            //This means the result is null which implies
            //that a throwable exists. In this case the read on the InputStream threw an IOException
            //First close the InputStream
            try {
              _inputStream.close();
            } catch (IOException ioException) {
              //todo can this exception be swallowed? Perhaps be logged?
              //An exception thrown when we try to close the InputStream should not really
              //make its way down as an error...
            }
            //Now mark is it an error
            _dataSourceHandle.error(_inputStreamReader._error);
          }
        } else {
          //there was a timeout
          try {
            //Close the input stream here since we won't be doing any more reading. This should also potentially
            //free up any threads blocked on the read()
            _inputStream.close();
          } catch (IOException ioException) {
            //todo can this exception be swallowed? Perhaps be logged?
          }
          _dataSourceHandle.error(new TimeoutException("InputStream reading timed out"));
        }
      } catch (InterruptedException exception) {

        //If this thread interrupted, then we have no choice but to abort everything
        try {
          //Close the input stream here since we won't be doing any more reading. This should also potentially
          //free up any threads blocked on the read() if a TimeoutException took place.
          _inputStream.close();
        } catch (IOException ioException) {
          //todo can this exception be swallowed? Perhaps be logged?
        }
        _dataSourceHandle.error(exception);
      }
    }
  }

  /**
   * Create a new instance of a MultiPartMIMEInputStream that wraps the provided InputStream to
   * construct an individual multipart MIME part within the multipart MIME envelope.
   * @param headers to be placed within this part
   * @param inputStream to be used to read bytes
   */
  //todo mention in java docs that the thread pool MUST have atleast two threads
  public static MultiPartMIMEInputStream createMultiPartMIMEInputStream(final Map<String, String> headers,
      final InputStream inputStream, final ExecutorService executorService) {
    return createMultiPartMIMEInputStream(headers, inputStream, executorService, DEFAULT_MAXIMUM_BLOCKING_DURATION);
  }

  /**
   * Create a new instance of a MultiPartMIMEInputStream that wraps the provided InputStream to
   * construct an individual multipart MIME part within the multipart MIME envelope.
   */
  public static MultiPartMIMEInputStream createMultiPartMIMEInputStream(final Map<String, String> headers,
      final InputStream inputStream, final ExecutorService executorService, final long maximumBlockingTime) {
    return new MultiPartMIMEInputStream(headers, inputStream, executorService, maximumBlockingTime);
  }


  public static class Builder {

    //USE A BUILDER_ REUSME HERE
    public Builder appendMultiPartMIMEDataSource(final MultiPartMIMEDataSource dataSource) {
      _dataSources.add(dataSource);
      return this;
    }

    public MultiPartMIMEWriter build() {
      return new MultiPartMIMEWriter(_dataSources);
    }
  }



  //Private construction due to the static factories. This also helps prevent subclassing since this class is
  //not designed to be subclassed
  private MultiPartMIMEInputStream(final Map<String, String> headers, final InputStream inputStream,
      final ExecutorService executorService, long maximumBlockingTime) {
    //todo defensive copy
    _headers = new HashMap<String, String>(headers);
    _inputStream = inputStream;
    _executorService = executorService;
    _maximumBlockingTime = maximumBlockingTime;
  }
}
