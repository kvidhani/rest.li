package com.linkedin.multipart.writer;

import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import java.util.ArrayList;
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
  private int _currentDataSource;
  private WriteHandle _writeHandle;

  private class R2MultiPartMIMEWriter implements Writer {

    @Override
    public void onInit(WriteHandle wh) {
      _writeHandle = wh;
      _currentDataSource = 0;
    }

    @Override
    public void onWritePossible() {
      //todo check to see if write handle is null?

      //Iterate through data sources one by one, get the bytes, and write to the write handle
      //make sure to fulfill all of the bytes requested
      while (_writeHandle.remaining()> 0) {
        final MultiPartMIMEDataSource currentDataSource = _dataSources.get(_currentDataSource);

        //final DataSourceHandle dataSourceHandle = new DataSourceHandleImpl(_writeHandle)
      }

    }

    @Override
    public void onAbort(Throwable e) {
      //Abort all data sources from the current data source going forward.
      //Also the current DataSourceHandle should be set to ABORTED
    }

    R2MultiPartMIMEWriter() {}
  }


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
}
