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
  private final MultiPartMIMEReader _multiPartMIMEReader;
  private MultiPartMIMEReader.SinglePartMIMEReaderDataSource _currentSinglePartReaderDataSource;
  private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
  private final byte[] _normalEncapsulationBoundary;

  @Override
  public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
    final SinglePartMIMEReaderCallback singlePartMIMEChainReader =
        new SinglePartMIMEReaderDataSourceCallback(_writeHandle, singlePartMIMEReader, this);
    //Wrap the SinglePartReader in the proxy class which implements the MultiPartMIMEDataSource interface
    _currentSinglePartReaderDataSource = new MultiPartMIMEReader.SinglePartMIMEReaderDataSource(singlePartMIMEReader);
    singlePartMIMEReader.registerReaderCallback(singlePartMIMEChainReader);

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
      _writeHandle.error(ioException); //Should never happen
      //todo what to place here
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
    //There was a problem reading, so therefore there is a problem writing.
    _writeHandle.error(e);
    //This reader is now in an unus
    //RESUME HERE TODO AND FIGURE ALL THIS OUT

    //TODO - open a JIRA so that we can invoke client callbacks of the result of their chaining
  }

  public MultiPartMIMEChainReaderCallback(final WriteHandle writeHandle,
                                          final MultiPartMIMEReader multiPartMIMEReader,
      final byte[] normalEncapsulationBoundary) {
    _writeHandle = writeHandle;
    _multiPartMIMEReader = multiPartMIMEReader;
    _normalEncapsulationBoundary = normalEncapsulationBoundary;
  }

  MultiPartMIMEReader.SinglePartMIMEReaderDataSource getCurrentSinglePartReaderDataSource() {
    return _currentSinglePartReaderDataSource;
  }
}
