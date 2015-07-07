package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;


/**
 * Created by kvidhani on 7/2/15.
 */
public class SinglePartMIMEReaderDataSourceCallback implements SinglePartMIMEReaderCallback {

  private final WriteHandle _writeHandle;
  private final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
  private final MultiPartMIMEReaderCallback _multiPartMIMEReaderCallback;

  @Override
  public void onPartDataAvailable(ByteString b) {
    _writeHandle.write(b);
    if(_writeHandle.remaining() > 0) {
      //No danger of a stack overflow due to the iterative invocation technique in MultiPartMIMEReader
      _singlePartMIMEReader.requestPartData();
    }
  }

  @Override
  public void onFinished() {
    _writeHandle.done();
  }

  @Override
  public void onAbandoned() {
    //This can be ignored. When a SinglePartMimeReader is used as a data source during chaining, we never
    //ask the part to be abandoned.
  }

  @Override
  public void onStreamError(Throwable e) {
    //If there was an error while this single part was being read then we notify the parent callback,
    //then we notify the writeHandle
    _multiPartMIMEReaderCallback.onStreamError(e);
    _writeHandle.error(e);
  }

  public SinglePartMIMEReaderDataSourceCallback(final WriteHandle writeHandle,
      final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader,
      final MultiPartMIMEReaderCallback multiPartMIMEReaderCallback) {
    _singlePartMIMEReader = singlePartMIMEReader;
    _writeHandle = writeHandle;
    _multiPartMIMEReaderCallback = multiPartMIMEReaderCallback;
  }
}
