package com.linkedin.multipart;

import com.linkedin.data.ByteString;

import com.linkedin.r2.message.streaming.WriteHandle;

import java.io.IOException;


/**
 * @author Karim Vidhani
 *
 * Callback registered by the {@link com.linkedin.multipart.MultiPartMIMEWriter} to chain this as a data source.
 */
class MultiPartMIMEChainReaderCallback implements MultiPartMIMEReaderCallback {
    private final WriteHandle _writeHandle;
    private MultiPartMIMEReader.SinglePartMIMEReader _currentSinglePartReader;
    private final byte[] _normalEncapsulationBoundary;

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
        //When each single part finishes we cannot notify the write handle that we are done.
        final SinglePartMIMEReaderCallback singlePartMIMEChainReader =
                new SinglePartMIMEChainReaderCallback(_writeHandle, singlePartMIMEReader, false);
        _currentSinglePartReader = singlePartMIMEReader;
        singlePartMIMEReader.registerReaderCallback(singlePartMIMEChainReader);

        ByteString serializedBoundaryAndHeaders = null;
        try {
            serializedBoundaryAndHeaders = MultiPartMIMEUtils.serializeBoundaryAndHeaders(_normalEncapsulationBoundary, singlePartMIMEReader);
        } catch (IOException ioException) {
            onStreamError(ioException); //Should never happen
        }

        _writeHandle.write(serializedBoundaryAndHeaders);
        if (_writeHandle.remaining() > 0) {
            singlePartMIMEReader.requestPartData();
        }
    }

    @Override
    public void onFinished() {
        _writeHandle.done();
    }

    @Override
    public void onAbandoned() throws UnsupportedOperationException {
        //Should never happen. The writer who is consuming this MultiPartMIMEReader as a data source should never
        //abandon data.
    }

    @Override
    public void onStreamError(Throwable throwable) {
        //If there was an error reading then we notify the writeHandle.
        //Note that the MultiPartMIMEReader and SinglePartMIMEReader have already been rendered
        //inoperable due to this. We just need to let the writeHandle know of this problem.

        //Also note that there may or may not be a current SinglePartMIMEReader. If there was
        //then it already invoked _writeHandle.error().
        //Regardless its safe to do it again in case this did not happen.

        //Lastly note there is no way to let an application developer know that their MultiPartMIMEReader
        //they sent further downstream had an error.
        _writeHandle.error(throwable);
    }

    MultiPartMIMEChainReaderCallback(final WriteHandle writeHandle,
                                            final byte[] normalEncapsulationBoundary) {
        _writeHandle = writeHandle;
        _normalEncapsulationBoundary = normalEncapsulationBoundary;
    }

    MultiPartMIMEReader.SinglePartMIMEReader getCurrentSinglePartReader() {
        return _currentSinglePartReader;
    }
}
