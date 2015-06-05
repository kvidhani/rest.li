package com.linkedin.multipart.reader;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.reader.exceptions.*;
import com.linkedin.multipart.writer.MultiPartMIMEDataSource;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;

import java.util.Map;

/**
 * Created by kvidhani on 5/18/15.
 */
public class MultiPartMIMEReader {


    //Hide the reader
    private final R2MultiPartMimeReader _reader;
    //Note that the reader callback will only be allowed to change if there is a downstream
    //writer that needs to take this stream over and read from it.
    private MultiPartMIMEReaderCallback _clientCallback;
    private ByteString _currentBuffer; //We will always be a little ahead
    private final EntityStream _entityStream;

    private class R2MultiPartMimeReader implements Reader
    {
        private ReadHandle _rh;

        @Override
        public void onInit(ReadHandle rh) {
            _rh = rh;
        }

        @Override
        public void onDataAvailable(ByteString data) {
            //buffer/parse data and call the client supplied callback(s)
        }

        @Override
        public void onDone() {

        }

        @Override
        public void onError(Throwable e) {

        }

        private R2MultiPartMimeReader() {}
    }

    public static MultiPartMIMEReader createAndAcquireStream(StreamRequest request,
                                                             MultiPartMIMEReaderCallback clientCallback) {
        return new MultiPartMIMEReader(request, clientCallback);
    }

    public static MultiPartMIMEReader createAndAcquireStream(StreamResponse response,
                                                             MultiPartMIMEReaderCallback clientCallback) {
        return new MultiPartMIMEReader(response, clientCallback);
    }

    public static MultiPartMIMEReader createAndAcquireStream(StreamRequest request) {
        return new MultiPartMIMEReader(request, null);
    }

    public static MultiPartMIMEReader createAndAcquireStream(StreamResponse response) {
        return new MultiPartMIMEReader(response, null);
    }

    private MultiPartMIMEReader(StreamRequest request, MultiPartMIMEReaderCallback clientCallback) {
        _reader = new R2MultiPartMimeReader();
        _entityStream = request.getEntityStream();
        if (clientCallback!=null) {
            _clientCallback = clientCallback;
            _entityStream.setReader(_reader);
        }
    }

    private MultiPartMIMEReader(StreamResponse response, MultiPartMIMEReaderCallback clientCallback) {
        _reader = new R2MultiPartMimeReader();
        _entityStream = response.getEntityStream();
        if(clientCallback!=null) {
            _clientCallback = clientCallback;
            _entityStream.setReader(_reader);
        }
    }

    public class SinglePartMIMEReader implements MultiPartMIMEDataSource {

        private final Map<String, String> _headers;
        private SinglePartMIMEReaderCallback _callback;

        //Only MultiPartMIMEReader should ever create an instance
        private SinglePartMIMEReader(Map<String, String> headers) {
            _headers = headers;
        }

        //This call commits and binds this callback to finishing this part. This can
        //only happen once per life of each SinglePartMIMEReader.
        //Meaning PartBindException will be thrown if there are attempts to mutate this callback
        public void registerReaderCallback(SinglePartMIMEReaderCallback callback) throws PartBindException {
            _callback = callback;
        }

        //Headers can be null/empty here if the part doesn't have any headers (since headers are not required)
        public Map<String, String> getHeaders() {
            return _headers;
        }

        //Read bytes from this part.
        //Throws IllegalArgumentException if num bytes falls outside of a certain range
        //or PartNotInitializedException if this API is called without init() performed
        //1. If call in the middle of a part, we return the bytes requested
        //on the IndividualPartReaderCallback via onPartDataAvailable().
        //2. If calling this many bytes would exactly finish off the current part, then
        //onPartDataAvailable() is called followed by onCurrentPartSuccessfullyFinished().
        //3. If calling this many bytes would exceed the current part, then the provided numBytes
        //will not be fully respected. In such a case onPartDataAvailable() is called with
        //the remaining bytes needed to finish off the current part, followed by onCurrentPartSuccessfullyFinished().
        //4. If the last part is finished off with this call, then onPartDataAvailable() is
        //called with the last remaining bytes, followed by onCurrentPartSuccessfullyFinished() followed by
        //allPartsFinished() in their respective callbacks.
        //5. If this part is fully consumed, meaning onCurrentPartSuccessfullyFinished() has been called,
        //then any subsequent calls to readPartData() will simply call
        //onCurrentPartSuccessfullyFinished() again.
        //6. Since this is async and we do not allow request queueing, repetitive calls will
        //result in StreamBusyException
        public void readPartData(int numBytes) throws
                IllegalArgumentException, PartNotInitializedException, StreamBusyException {

        }


        //Abandon the current part.
        //We read up until the next part and drop all bytes we encounter.
        //Once abandonment is done we call onCurrentPartAbandoned() on the
        //SinglePartMIMEReader callback.
        //This API can be called before the init() call. In such cases, since there is no
        //callback invoked when the abandonment is finished, since no callback was registered.
        //1. If this part is finished, meaning onCurrentPartAbandoned() or
        //onCurrentPartSuccessfullyFinished() has already been called
        //already, then a call to abandonPart() will throw PartFinishedException
        //3. Clients who call this cannot provide a numBytes, since we will be doing the reading
        //and the dropping.
        //4. Since this is async and we do not allow request queueing, repetitive calls will
        //result in StreamBusyException
        public void abandonPart() throws PartFinishedException, StreamBusyException {

        }

    }

    //True only if allPartsFinished() has not yet been called on the provided callback
    //Does not block
    public boolean haveAllPartsFinished() {
        return false; //todo
    }

    //Reads through and abandons the new part and additionally the whole stream.
    //This can ONLY be called if there is no part being actively read, meaning that
    //the current SinglePartMIMEReader has not been initialized with an SingelPartMIMEReaderCallback.
    //If this is violated we throw a StreamBusyException.
    //1. Once the stream is finished being abandoned, we call allPartsAbandoned().
    //2. If the stream is finished, subsequent calls will throw StreamFinishedException
    //3. Since this is async and we do not allow request queueing, repetitive calls will
    //result in StreamBusyException.
    //4. If this MultiPartMIMEReader was created without a callback, and none has been registered yet
    //then a call to abanonAllParts() will result in a ReaderNotInitializedException.
    public void abandonAllParts() throws StreamBusyException, StreamFinishedException, ReaderNotInitializedException {

    }

    //Package Private reader callback registration
    //Used ONLY by a MultiPartMIMEWriter when they want to take over the rest of this stream.
    //This can ONLY be called if there is no part being actively read, meaning that
    //the current SinglePartMIMEReader has had no callback registered. Violation of this
    //will throw StreamBusyException.
    //This can be set even if no parts in the stream have actually been consumed, i.e
    //after the very first invocation of onNewPart() on the initial MultiPartMIMEReaderCallback.
    void registerReaderCallback(MultiPartMIMEReaderCallback clientCallback) throws StreamBusyException {
        _clientCallback = clientCallback;
        _entityStream.setReader(_reader);
    }

}
