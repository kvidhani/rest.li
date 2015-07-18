package com.linkedin.multipart;

import com.linkedin.data.ByteString;

import com.linkedin.r2.message.streaming.WriteHandle;
import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * Created by kvidhani on 7/2/15.
 */
public class MultiPartMIMEChainReaderCallback implements MultiPartMIMEReaderCallback
{
  private final WriteHandle _writeHandle;
  //private final MultiPartMIMEReader _multiPartMIMEReader;
  private MultiPartMIMEReader.SinglePartMIMEReader _currentSinglePartReader;
  private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
  private final byte[] _normalEncapsulationBoundary;

  @Override
  public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
    final SinglePartMIMEReaderCallback singlePartMIMEChainReader =
        new SinglePartMIMEChainReaderCallback(_writeHandle, singlePartMIMEReader);
    _currentSinglePartReader = singlePartMIMEReader;
    singlePartMIMEReader.registerReaderCallback(singlePartMIMEChainReader);

    //todo relocate this logic so its easier to test
    byteArrayOutputStream.reset();

    try {
      //Write the headers out for this new part
      byteArrayOutputStream.write(_normalEncapsulationBoundary);
      byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);

      if (!singlePartMIMEReader.dataSourceHeaders().isEmpty()) {
        //Serialize the headers
        byteArrayOutputStream
            .write(MultiPartMIMEUtils.serializedHeaders(singlePartMIMEReader.dataSourceHeaders()).copyBytes());
      }

      //Regardless of whether or not there were headers the RFC calls for another CRLF here.
      //If there were no headers we end up with two CRLFs after the boundary
      //If there were headers CRLF_BYTES we end up with one CRLF after the boundary and one after the last header
      byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);
    } catch (IOException ioException) {
      onStreamError(ioException); //Should never happen
    }

    _writeHandle.write(ByteString.copy(byteArrayOutputStream.toByteArray()));
    if (_writeHandle.remaining() > 0) {
      singlePartMIMEReader.requestPartData();
    }
  }

  @Override
  public void onFinished() {
    //This particular entity stream is done
    _writeHandle.done();
  }

  @Override
  public void onAbandoned() throws UnsupportedOperationException {
    //Should never happen. The writer who is consuming this MultiPartMIMEReader as a data source should never
    //abandon data.
  }

  @Override
  public void onStreamError(Throwable e) {

    //If there was an error reading then we notify the writeHandle.
    //Note that the MultiPartMIMEReader and SinglePartMIMEReader have already been rendered
    //inoperable due to this. We just need to let the writeHandle know of this problem.

    //Also note that there may or may not be a current SinglePartMIMEReader. If there was
    //then it already invoked _writeHandle.error(). See SinglePartMIMEReaderDataSourceCallback.
    //Regardless its safe to do it again in case this did not happen.

    //Lastly note there is no way to let an application developer know that they MultiPartMIMEReader
    //they sent further downstream had an error.
    _writeHandle.error(e);
    //TODO - open a JIRA so that we can invoke client callbacks of the result of their chaining
  }

  public MultiPartMIMEChainReaderCallback(final WriteHandle writeHandle,
      final byte[] normalEncapsulationBoundary) {
    _writeHandle = writeHandle;
    _normalEncapsulationBoundary = normalEncapsulationBoundary;
  }

  MultiPartMIMEReader.SinglePartMIMEReader getCurrentSinglePartReader() {
    return _currentSinglePartReader;
  }
}
