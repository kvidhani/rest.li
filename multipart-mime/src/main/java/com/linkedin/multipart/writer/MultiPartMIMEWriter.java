package com.linkedin.multipart.writer;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.MultiPartMIMEUtils;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;


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
  private volatile boolean _transitionToNewPart = true;

  //Our implementation of a R2 writer
  private class R2MultiPartMIMEWriter implements Writer {
    private boolean _preambleWritten = false;
    private WriteHandle _writeHandle;
    private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    @Override
    public void onInit(WriteHandle wh) {
      _writeHandle = wh;
    }

    //todo would this cause a stack overflow
    @Override
    public void onWritePossible() {

      while (_writeHandle.remaining() > 0) {

        byteArrayOutputStream.reset();

        try {
          if (!_preambleWritten) {
            //RFC states that an optional preamble can be supplied before the first boundary for the first part.
            //This is to be ignored but its helpful for non-MIME compliant readers to see what's going on
            byteArrayOutputStream.write(_preamble.getBytes(Charset.forName("US-ASCII")));
            byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF);
            _preambleWritten = true;
          }
          //If we have finished all our data sources
          if (_currentDataSource > _dataSources.size() - 1) {

            //We write the last boundary with an extra two hyphen characters according to the RFC
            //We then write the epilogue and then we call onDone() on the WriteHandle
            byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF);
            byteArrayOutputStream.write(_finalEncapsulationBoundary);
            byteArrayOutputStream.write(_epilogue.getBytes(Charset.forName("US-ASCII")));
            _writeHandle.write(ByteString.copy(byteArrayOutputStream.toByteArray()));
            _writeHandle.done();

          } else {

            final MultiPartMIMEDataSource currentDataSource = _dataSources.get(_currentDataSource);

            //Transitions to new parts will happen once the current data source has called onDone() on the DataSourceHandle
            if (_transitionToNewPart) {
              //On each transition to a new part, write a boundary first. The CRLF before the boundary and after
              //the boundary is considered part of the boundary
              byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF);
              byteArrayOutputStream.write(_normalEncapsulationBoundary);
              byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF);

              if (!currentDataSource.dataSourceHeaders().isEmpty()) {
                //Serialize the headers
                byteArrayOutputStream.write(MultiPartMIMEUtils.serializedHeaders(currentDataSource.dataSourceHeaders()).copyBytes());
              }

              //Regardless of whether or not there were headers the RFC calls for another CRLF here.
              //If there were no headers we end up with two CRLFs after the boundary
              //If there were headers then we end up with one CRLF after the boundary and one after the last header
              byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF);

              //Init the data source, letting them know that they are about to be called
              final DataSourceHandle dataSourceHandle =
                  new DataSourceHandleImpl(_writeHandle, MultiPartMIMEWriter.this);
              currentDataSource.onInit(dataSourceHandle);

              _writeHandle.write(ByteString.copy(byteArrayOutputStream.toByteArray()));

              //We are done transitioning to a new part
              _transitionToNewPart = false;
            } else {
              //Iterate through data sources one by one, get the bytes, and write to the write handle
              //using the DataSourceHandle as a proxy.
              currentDataSource.onWritePossible();
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
    _transitionToNewPart = true;
  }
}