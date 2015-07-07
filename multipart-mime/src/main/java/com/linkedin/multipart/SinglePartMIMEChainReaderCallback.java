package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;


/**
 * Created by kvidhani on 7/2/15.
 */
public class SinglePartMIMEChainReaderCallback implements SinglePartMIMEReaderCallback {

  private final WriteHandle _writeHandle;
  private final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;

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
    //If there was an error while this single part was being read then we notify the writeHandle.
    //Note that the MultiPartMIMEReader and SinglePartMIMEReader have already been rendered
    //inoperable due to this. We just need to let the writeHandle know of this problem.
    //Also note that the MultiPartMIMEReaderCallback that was associated with this SinglePartMIMEReader
    //has also been informed of this. Therefore if an application developer had chosen to send just
    //this part further down they could recover as well.
    _writeHandle.error(e);
  }

  public SinglePartMIMEChainReaderCallback(final WriteHandle writeHandle,
                                           final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
    _singlePartMIMEReader = singlePartMIMEReader;
    _writeHandle = writeHandle;
  }
}
