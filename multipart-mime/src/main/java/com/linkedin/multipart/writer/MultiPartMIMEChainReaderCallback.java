package com.linkedin.multipart.writer;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.MultiPartMIMEUtils;
import com.linkedin.multipart.reader.MultiPartMIMEReader;
import com.linkedin.multipart.reader.MultiPartMIMEReaderCallback;
import com.linkedin.multipart.reader.SinglePartMIMEReaderCallback;
import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * Created by kvidhani on 7/2/15.
 */
public class MultiPartMIMEChainReaderCallback implements MultiPartMIMEReaderCallback
{
  private final MultiPartMIMEWriter.DataSourceHandleImpl _dataSourceHandle;
  private MultiPartMIMEReader.SinglePartMIMEReader _currentSinglePartReader;
  private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
  private final byte[] _normalEncapsulationBoundary;

  @Override
  public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader) {
    final SinglePartMIMEReaderCallback singlePartMIMEChainReader = new SinglePartMIMEChainReaderCallback(_dataSourceHandle);
    _currentSinglePartReader = singleParMIMEReader;
    singleParMIMEReader.registerReaderCallback(singlePartMIMEChainReader);

    byteArrayOutputStream.reset();

    try {
      //Write the headers out for this new part
      byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);
      byteArrayOutputStream.write(_normalEncapsulationBoundary);
      byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);

      if (!singleParMIMEReader.dataSourceHeaders().isEmpty()) {
        //Serialize the headers
        byteArrayOutputStream
            .write(MultiPartMIMEUtils.serializedHeaders(singleParMIMEReader.dataSourceHeaders()).copyBytes());
      }

      //Regardless of whether or not there were headers the RFC calls for another CRLF here.
      //If there were no headers we end up with two CRLFs after the boundary
      //If there were headers CRLF_BYTES we end up with one CRLF after the boundary and one after the last header
      byteArrayOutputStream.write(MultiPartMIMEUtils.CRLF_BYTES);
    } catch (IOException ioException) {
      _dataSourceHandle.error(ioException); //Should never happen
    }

    _dataSourceHandle.write(ByteString.copy(byteArrayOutputStream.toByteArray()));
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

  public MultiPartMIMEChainReaderCallback(final MultiPartMIMEWriter.DataSourceHandleImpl dataSourceHandle,
      final byte[] normalEncapsulationBoundary) {
    _dataSourceHandle = dataSourceHandle;
    _normalEncapsulationBoundary = normalEncapsulationBoundary;
  }

  MultiPartMIMEReader.SinglePartMIMEReader getCurrentSinglePartReader() {
    return _currentSinglePartReader;
  }
}
