package com.linkedin.multipart.writer;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.sun.mail.util.LineOutputStream;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;


/**
 * Created by kvidhani on 5/18/15.
 */
//todo mention thread safety for all of this
//todo redo javadocs

public final class MultiPartMIMEWriter {

  private final R2MultiPartMIMEWriter _writer;
  private final EntityStream _entityStream;
  private final List<MultiPartMIMEDataSource> _dataSources;
  private String _boundary;
  private WriteHandle _writeHandle;

  private class R2MultiPartMIMEWriter implements Writer {

    private int _currentDataSource;

    @Override
    public void onInit(WriteHandle wh) {
      _writeHandle = wh;
      _currentDataSource = 0; //signifies we need to populate the headers
    }

    @Override
    public void onWritePossible() {
      //todo check to see if write handle is null?

      //todo - on the first call epilogue

      //on each transition to a new part, write a boundary first
      //when finished write out the last boundary
      //keep in mind the hyphens

      //Iterate through data sources one by one, get the bytes, and write to the write handle
      //make sure to fulfill all of the bytes requested? is this still needed?


      while (_writeHandle.remaining() > 0) {
        final MultiPartMIMEDataSource currentDataSource = _dataSources.get(_currentDataSource);
        final DataSourceHandle dataSourceHandle = new DataSourceHandleImpl(_writeHandle);
        LineOutputStream a = null;
      }
    }

    //todo handle preamble
    private ByteString serializedHeaders(final Map<String, String> headers) {

      final StringBuffer headerBuffer = new StringBuffer();
      for (final Map.Entry<String, String> header : headers.entrySet()) {
        headerBuffer.append(formattedHeader(header.getKey(), header.getValue()));
      }

      //Headers should always be 7 bit ASCII according to the RFC
      return ByteString.copyString(headerBuffer.toString(), Charset.forName("US-ASCII"));
    }

    private String formattedHeader(final String name, final String value)
    {
      return ((name == null ? "" : name)
              + ": "
              + (null == value ? "" : value)
              + "\r\n");
    }



    @Override
    public void onAbort(Throwable e) {
      //Abort all data sources from the current data source going forward.
      //Also the current DataSourceHandle should be set to ABORTED
    }

    R2MultiPartMIMEWriter() {
    }
  }

  //Javadocs will mention the need for clients to pass in content type explicitly
  //any content-type passed in headers will be overwritten
  //todo preamble and epilogue
  public MultiPartMIMEWriter(final MultiPartMIMEDataSource dataSource) {

    this(Arrays.asList(dataSource));
  }

  public MultiPartMIMEWriter(final List<MultiPartMIMEDataSource> dataSources) {

    _dataSources = dataSources;
    _writer = new R2MultiPartMIMEWriter();
    _entityStream = EntityStreams.newEntityStream(_writer);

  }

  public EntityStream getEntityStream() {
    return _entityStream;
  }

  //Package private and set by MultiPartMIMEStreamRequestBuilder
  void setBoundary(final String boundary) {
    _boundary = boundary;
  }
}
