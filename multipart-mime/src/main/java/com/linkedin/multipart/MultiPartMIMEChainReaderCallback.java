package com.linkedin.multipart;

import com.linkedin.data.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * Created by kvidhani on 7/2/15.
 */
public class MultiPartMIMEChainReaderCallback implements MultiPartMIMEReaderCallback
{
  private final MultiPartMIMEWriter.DataSourceHandleImpl _dataSourceHandle;
  private final MultiPartMIMEReader _multiPartMIMEReader;
  private MultiPartMIMEReader.SinglePartMIMEReaderDataSource _currentSinglePartReaderDataSource;
  private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
  private final byte[] _normalEncapsulationBoundary;

  @Override
  public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader) {
    final SinglePartMIMEReaderCallback singlePartMIMEChainReader = new SinglePartMIMEReaderDataSourceCallback(_dataSourceHandle);
    //Wrap the SinglePartReader in the proxy class which implements the MultiPartMIMEDataSource interface
    _currentSinglePartReaderDataSource = new MultiPartMIMEReader.SinglePartMIMEReaderDataSource(singleParMIMEReader);
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
    //This can be ignored. This is if the MultiPartMIMEReader this is registered to is passed as a data source to the writer
    //and the writer finishes consuming all the parts.
  }

  @Override
  public void onAbandoned() throws UnsupportedOperationException {
    //Should never happen. The writer who is consuming this MultiPartMIMEReader as a data source should never
    //abandon data.
  }

  @Override
  public void onStreamError(Throwable e) {
    //This will be invoked if any part within this MultiPartMIMEReader was told to abort.
    //This allows clients who sent this MultiPartMIMEReader to a writer as a data source to be notified
    //and potentially clean up.

    //We need to have behavior similar to handleExceptions() so that everything is cancelled
    //If there were potentially multiple chains across different servers, then all the readers
    //in the chain need to be shut down.
    //This is in contrast to the case where if one SinglePartReader was sent down as a data source. In that
    //case we notify the custom client MultiPartMIMEReaderCallback and they can recover.

    //todo test this! todo is this really correct behavior
    _multiPartMIMEReader.getR2MultiPartMIMEReader().handleExceptions(e);
  }

  public MultiPartMIMEChainReaderCallback(final MultiPartMIMEWriter.DataSourceHandleImpl dataSourceHandle,
                                          final MultiPartMIMEReader multiPartMIMEReader,
      final byte[] normalEncapsulationBoundary) {
    _dataSourceHandle = dataSourceHandle;
    _multiPartMIMEReader = multiPartMIMEReader;
    _normalEncapsulationBoundary = normalEncapsulationBoundary;
  }

  MultiPartMIMEReader.SinglePartMIMEReaderDataSource getCurrentSinglePartReaderDataSource() {
    return _currentSinglePartReaderDataSource;
  }
}
