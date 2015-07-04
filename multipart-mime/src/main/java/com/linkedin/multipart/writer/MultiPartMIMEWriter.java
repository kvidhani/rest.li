package com.linkedin.multipart.writer;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.MultiPartMIMEUtils;
import com.linkedin.multipart.reader.MultiPartMIMEReader;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.r2.util.LinkedDeque;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;


/**
 * Created by kvidhani on 5/18/15.
 */
//todo mention thread safety for all of this
//todo redo javadocs

public final class MultiPartMIMEWriter {

  private final R2MultiPartMIMEWriter _writer;
  private final EntityStream _entityStream;
  private final List<MultiPartMIMEDataSource> _dataSources;
  private final String _rawBoundary;
  private final byte[] _normalEncapsulationBoundary;
  private final byte[] _finalEncapsulationBoundary;
  private final String _preamble;
  private final String _epilogue;

  private volatile int _currentDataSource = 0;
  private volatile boolean _transitionToNewDataSource = true;

  //Our implementation of a R2 writer
  private class R2MultiPartMIMEWriter implements Writer {

    //A flag to indicate whethr or not the preamble has been written
    private boolean _preambleWritten = false;
    //The write handle to write to that will be provided by R2
    private WriteHandle _writeHandle;
    //The mutable byte array we use to create the ByteString which we will end up writing
    private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    //The current MultiPartMIMEChainReaderCallback in the case that our data source is a MultiPartMIMEReader. If our
    //current data source is a MultiPartMIMEReader then we will potentially walk through many SinglePartMIMEReaders.
    //If this happens, we will need to know the current SinglePartMIMEReader (similar to how we need to know the current
    //MultiPartMIMEDataSource). Therefore we need to keep track of the current MultiPartMIMEReaderCallback because it is
    //this callback that maintains a reference to the current SinglePartMIMEReader. We need this reference so that we
    //can ask it to read the data and then write the result of that read to the writeHandle.
    private volatile MultiPartMIMEChainReaderCallback _currentMultiPartMIMEReaderCallback;

    //These are needed to support our iterative invocation of onWritePossible so that we don't end up with a recursive loop
    //which would lead to a stack overflow. This could happen if R2 asks us to write many chunks.
    private final Queue<Callable> _callbackQueue = new LinkedDeque<Callable>();
    private volatile boolean _callbackInProgress = false;

    private void processAndInvokeCallableQueue() {

      //This variable indicates that there is no current iterative invocation taking place. We can start one here.
      _callbackInProgress = true;

      while (!_callbackQueue.isEmpty()) {
        final Callable<Void> callable = _callbackQueue.poll();
        try {
          callable.call();
        } catch (Exception clientCallbackException) {
          //todo is this correct?
          _writeHandle.error(clientCallbackException);
        }
      }
      _callbackInProgress = false;
    }


    @Override
    public void onInit(WriteHandle wh) {
      _writeHandle = wh;
    }

    //todo change the logic so that the CRLF because the first thing in a boundary - this semantic more closely matches the RFC
    //todo note that we need to mention that remaining()>0 thing in explicit details.
    @Override
    public void onWritePossible() {

      //If there is more to process we keep moving forward. If this is the last write on the write handle
      //which brought remaining down to 0, then we don't do anything else. Subsequent requests to write more data
      //by the reader will then have r2 call onWritePossible().
      while (_writeHandle.remaining() > 0) {

        byteArrayOutputStream.reset();

        //We can only write once per iteration of this loop.
        try {
          if (!_preambleWritten) {
            //RFC states that an optional preamble can be supplied before the first boundary for the first part.
            //This is to be ignored but its helpful for non-MIME compliant readers to see what's going on
            byteArrayOutputStream.write(_preamble.getBytes(Charset.forName("US-ASCII")));
            byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);
            _preambleWritten = true;
            _writeHandle.write(ByteString.copy(byteArrayOutputStream.toByteArray()));
          } else {
            //If we have finished all our data sources
            if (_currentDataSource > _dataSources.size() - 1) {

              //We write the last boundary with an extra two hyphen characters according to the RFC
              //We then write the epilogue and then we call onDone() on the WriteHandle
              byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);
              byteArrayOutputStream.write(_finalEncapsulationBoundary);
              byteArrayOutputStream.write(_epilogue.getBytes(Charset.forName("US-ASCII")));
              _writeHandle.write(ByteString.copy(byteArrayOutputStream.toByteArray()));
              _writeHandle.done();
            } else {

              //todo fix this and APIs
              //TODO - RESUME HERE
              final MultiPartMIMEDataSource currentDataSource = _dataSources.get(_currentDataSource);

              if (currentDataSource instanceof MultiPartMIMEDataSource) {
                //Transitions to new parts will happen once the current data source has called onDone() on the DataSourceHandle
                if (_transitionToNewDataSource) {
                  //On each transition to a new part, write a boundary. The CRLF before the boundary and after
                  //the boundary is considered part of the boundary.
                  //The CRLF after the boundary is considered the beginning of the first header
                  byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);
                  byteArrayOutputStream.write(_normalEncapsulationBoundary);
                  byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);

                  if (!currentDataSource.dataSourceHeaders().isEmpty()) {
                    //Serialize the headers
                    byteArrayOutputStream
                            .write(MultiPartMIMEUtils.serializedHeaders(currentDataSource.dataSourceHeaders()).copyBytes());
                  }

                  //Regardless of whether or not there were headers the RFC calls for another CRLF here.
                  //If there were no headers we end up with two CRLFs after the boundary
                  //If there were headers CRLF_BYTES we end up with one CRLF after the boundary and one after the last header
                  byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);

                  //Init the data source, letting them know that they are about to be called
                  final DataSourceHandleImpl dataSourceHandle = new DataSourceHandleImpl(_writeHandle, this);
                  currentDataSource.onInit(dataSourceHandle);

                  _writeHandle.write(ByteString.copy(byteArrayOutputStream.toByteArray()));

                  //We are done transitioning to a new part
                  _transitionToNewDataSource = false;
                } else {

                  //Now notify the data source to write to the write handle using the DataSourceHandle as a proxy.
                  //currentDataSource.onWritePossible();
                  final Callable<Void> onWritePossibleInvocation = new MimeWriterCallables.onWritePossibleCallable(currentDataSource);

                  //Queue up this operation
                  _callbackQueue.add(onWritePossibleInvocation);

                  //If the while loop before us is in progress, we just return
                  if (_callbackInProgress) {
                    //We return to unwind the stack. Any queued elements will be taken care of the by the while loop
                    //before us.
                    return;
                  } else {
                    processAndInvokeCallableQueue();
                  }
                  return; //We return. We will come back to this method when the write we just scheduled
                  //has finished.
                }
              } else {

                //current data source is a multi part mime reader

                if (_transitionToNewDataSource) {

                  final MultiPartMIMEReader reader = (MultiPartMIMEReader) currentDataSource;
                  final DataSourceHandleImpl dataSourceHandle = new DataSourceHandleImpl(_writeHandle, this);
                  _currentMultiPartMIMEReaderCallback = new MultiPartMIMEChainReaderCallback(dataSourceHandle, _normalEncapsulationBoundary);
                  //Since this is not a MultiPartMIMEDataSource we can't use the regular mechanism for reading data.
                  //Instead of create a new callback that will use write to the writeHandle using the SinglePartMIMEReader

                  reader.registerReaderCallback(_currentMultiPartMIMEReaderCallback);
                  //Note that by registering here, this will eventually lead to onNewPart() which will then requestPartData()
                  //which will eventually lead to onPartDataAvailable() which will then write to the writeHandle thereby
                  //honoring the original request here to write data. This initial write here will write out the boundary that this
                  //writer is using followed by the headers. This is similar to _transitionToNewDataSource except within
                  //a MultiPartMIMEReader.

                  _transitionToNewDataSource = false;
                  return; //We return. We will come back to this method when the write we just scheduled
                  //has finished.
                } else {
                  //_currentMultiPartMIMEReaderCallback.getCurrentSinglePartReader().onWritePossible();
                  final Callable<Void> onWritePossibleInvocation = new MimeWriterCallables.onWritePossibleCallable(_currentMultiPartMIMEReaderCallback.getCurrentSinglePartReader());

                  //Queue up this operation
                  _callbackQueue.add(onWritePossibleInvocation);

                  //If the while loop before us is in progress, we just return
                  if (_callbackInProgress) {
                    //We return to unwind the stack. Any queued elements will be taken care of the by the while loop
                    //before us.
                    return;
                  } else {
                    processAndInvokeCallableQueue();
                  }
                  return; //We return. We will come back to this method when the write we just scheduled
                  //has finished.
                }
              }
            }
          }
        } catch (IOException ioException) {
          //This should never happen. It signifies we had a problem writing to our ByteArrayOutputStream
          _writeHandle.error(ioException);
          onAbort(ioException); //We should clean up all of our data sources
        }
      }
    }


    @Override
    public void onAbort(Throwable e) {
      abortAllDataSources(e);
    }

    private R2MultiPartMIMEWriter() {
    }
  }

  void abortAllDataSources(final Throwable throwable)
  {
    //Abort all data sources from the current data source going forward
    for (int i = _currentDataSource; i<_dataSources.size(); i++)
    {
      _dataSources.get(i).onAbort(throwable);
    }

    //Note that a consumer could continue to use the DataSourceHandle to call write(), onDone() or error()
    //These will work as they did pre-abort, but r2 will just silently ignore them since the stream has already been aborted.
  }

  public MultiPartMIMEWriter(final MultiPartMIMEDataSource dataSource, final String preamble, final String epilogue) {
    this(Arrays.asList(dataSource), preamble, epilogue);
  }

  public MultiPartMIMEWriter(final List<MultiPartMIMEDataSource> dataSources, final String preamble,
      final String epilogue) {

    _dataSources = dataSources;
    _writer = new R2MultiPartMIMEWriter();
    _entityStream = EntityStreams.newEntityStream(_writer);
    _preamble = preamble;
    _epilogue = epilogue;

    //Create the boundary.
    _rawBoundary = MultiPartMIMEUtils.generateBoundary();
    //As per the RFC there must two preceding hyphen characters on each boundary between each parts
    _normalEncapsulationBoundary = ("--" + _rawBoundary).getBytes(Charset.forName("US-ASCII"));
    //As per the RFC the final boundary has two extra hyphens at the end
    _finalEncapsulationBoundary = ("--" + _rawBoundary + "--").getBytes(Charset.forName("US-ASCII"));
  }

  public EntityStream getEntityStream() {
    return _entityStream;
  }

  String getBoundary() {
    return _rawBoundary;
  }

  //Package private and set by the DataSourceHandle
  void currentDataSourceFinished()
  {
    _currentDataSource++;
    _transitionToNewDataSource = true;
  }

  /**
   * Created by kvidhani on 5/19/15.
   */
  //todo update this for thread safety...perhaps with atomic references?
  //todo redo javadocs

  public class DataSourceHandleImpl //implements DataSourceHandle
  {
    private final WriteHandle _writeHandle;
    private final R2MultiPartMIMEWriter _r2MultiPartMIMEWriter;
    private HandleState _state;

    //Package private construction
    DataSourceHandleImpl(final WriteHandle writeHandle, final R2MultiPartMIMEWriter r2MultiPartMIMEWriter)
    {
      _writeHandle = writeHandle;
      _r2MultiPartMIMEWriter = r2MultiPartMIMEWriter;
      _state = HandleState.ACTIVE;
    }

    public void write(final ByteString data) throws IllegalStateException, IllegalArgumentException
    {
      if(_state == HandleState.CLOSED)
      {
        throw new IllegalStateException("This DataSourceHandle is closed");
      }

      _writeHandle.write(data);
      //Try to keep moving forward in case there is more requests to write on the writeHandle remaining.
      _r2MultiPartMIMEWriter.onWritePossible();

    }

    public void done(ByteString remainingData) throws IllegalStateException
    {
      if(_state == HandleState.CLOSED)
      {
        throw new IllegalStateException("This DataSourceHandle is closed");
      }

      _state = HandleState.CLOSED;
      //We first transition the data source. Otherwise if we write first, then a new call to onWritePossible() could
      //occur in the writer inside of MultiPartMIMEWriter before our transition finishes.
      MultiPartMIMEWriter.this.currentDataSourceFinished();
      _writeHandle.write(remainingData);
      //Try to keep moving forward in case there is more requests to write on the writeHandle remaining.
      _r2MultiPartMIMEWriter.onWritePossible();

    }

    public void error(Throwable throwable)
    {
      _state = HandleState.CLOSED;
      _writeHandle.error(throwable);
      //If this data source has encountered an error everyone else in front of them needs to know.
      //Also note that exceptions thrown here
      //todo what happens if exceptions thrown here
      MultiPartMIMEWriter.this.abortAllDataSources(throwable);
    }


  }
  enum HandleState
  {
    ACTIVE, //Eligible to write bytes out
    CLOSED //This data source has called done(), or it has called error()
  }
}