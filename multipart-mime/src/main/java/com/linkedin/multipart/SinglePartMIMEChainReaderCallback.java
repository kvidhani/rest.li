package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.WriteHandle;


/**
 * @author Karim Vidhani
 *
 * Used to chain a {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader} as a data source
 * when creating a {@link com.linkedin.multipart.MultiPartMIMEWriter}.
 *
 * This class can be used by:
 * 1. When chaining along a {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader} directly. In
 * such a case we close the write handle upon invocation of {@link SinglePartMIMEChainReaderCallback#onFinished()}
 *
 * 2. When chaining along a top level {@link com.linkedin.multipart.MultiPartMIMEReader}. In this case the
 * {@link com.linkedin.multipart.MultiPartMIMEChainReaderCallback} will close the write handle when it recieves
 * an invocation on {@link MultiPartMIMEReaderCallback#onFinished()}.
 */
final class SinglePartMIMEChainReaderCallback implements SinglePartMIMEReaderCallback {
  private final WriteHandle _writeHandle;
  private final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
  private final boolean _doneOnFinished;

  @Override
  public void onPartDataAvailable(ByteString partData) {
    _writeHandle.write(partData);
    if(_writeHandle.remaining() > 0) {
      //No danger of a stack overflow due to the iterative invocation technique in MultiPartMIMEReader
      _singlePartMIMEReader.requestPartData();
    }
  }

  @Override
  public void onFinished() {
    if (_doneOnFinished) {
      _writeHandle.done();
    }
  }

  @Override
  public void onAbandoned() {
    //This can be ignored. A request to abandon is never performed during chaining.
  }

  @Override
  public void onStreamError(Throwable throwable) {
    //If there was an error while this single part was being read then we notify the writeHandle.
    //Note that the MultiPartMIMEReader and SinglePartMIMEReader have already been rendered
    //inoperable due to this. We just need to let the writeHandle know of this problem.
    //Also note that the MultiPartMIMEReaderCallback that was associated with this SinglePartMIMEReader
    //has also been informed of this. Therefore if an application developer had chosen to send just
    //this part further down they could recover as well.
    _writeHandle.error(throwable);
  }

  SinglePartMIMEChainReaderCallback(final WriteHandle writeHandle,
                                           final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader,
                                           final boolean doneOnFinished) {
    _singlePartMIMEReader = singlePartMIMEReader;
    _writeHandle = writeHandle;
    _doneOnFinished = doneOnFinished;
  }
}