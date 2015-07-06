package com.linkedin.multipart;

import com.linkedin.data.ByteString;


/**
 * Created by kvidhani on 7/2/15.
 */
public class SinglePartMIMEReaderDataSourceCallback implements SinglePartMIMEReaderCallback {

  private final MultiPartMIMEWriter.DataSourceHandleImpl _dataSourceHandle;

  @Override
  public void onPartDataAvailable(ByteString b) {
    _dataSourceHandle.write(b);
  }

  @Override
  public void onFinished() {
    _dataSourceHandle.done(ByteString.empty());
  }

  @Override
  public void onAbandoned() {
    //This can be ignored. When a SinglePartMimeReader is used as a data source during chaining, we never
    //ask the part to be abandoned.
  }

  @Override
  public void onStreamError(Throwable e) {
    //If there was an error while this single part was being read then we pass then on to the write handle
    //as we write.
    _dataSourceHandle.error(e);
  }

  public SinglePartMIMEReaderDataSourceCallback(final MultiPartMIMEWriter.DataSourceHandleImpl dataSourceHandle) {
    _dataSourceHandle = dataSourceHandle;
  }
}
