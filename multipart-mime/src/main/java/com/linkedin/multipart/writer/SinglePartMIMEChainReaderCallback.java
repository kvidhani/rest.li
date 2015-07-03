package com.linkedin.multipart.writer;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.reader.SinglePartMIMEReaderCallback;


/**
 * Created by kvidhani on 7/2/15.
 */
public class SinglePartMIMEChainReaderCallback implements SinglePartMIMEReaderCallback {

  private final MultiPartMIMEWriter.DataSourceHandleImpl _dataSourceHandle;

  @Override
  public void onPartDataAvailable(ByteString b) {
    _dataSourceHandle.write(b);
  }

  @Override
  public void onFinished() {

  }

  @Override
  public void onAbandoned() {

  }

  @Override
  public void onStreamError(Throwable e) {

  }

  public SinglePartMIMEChainReaderCallback(final MultiPartMIMEWriter.DataSourceHandleImpl dataSourceHandle) {
    _dataSourceHandle = dataSourceHandle;
  }
}

