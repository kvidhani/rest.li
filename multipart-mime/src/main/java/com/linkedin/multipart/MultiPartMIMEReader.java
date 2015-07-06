package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.reader.exceptions.IllegalMimeFormatException;
import com.linkedin.multipart.reader.exceptions.PartBindException;
import com.linkedin.multipart.reader.exceptions.PartFinishedException;
import com.linkedin.multipart.reader.exceptions.PartNotInitializedException;
import com.linkedin.multipart.reader.exceptions.ReaderNotInitializedException;
import com.linkedin.multipart.reader.exceptions.StreamBusyException;
import com.linkedin.multipart.reader.exceptions.StreamFinishedException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;
import com.linkedin.r2.message.streaming.Reader;
import com.linkedin.r2.util.LinkedDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.ArrayUtils;


/**
 * Created by kvidhani on 5/18/15.
 */


//Note that the reader is always in one of two states:
//1. Between parts. This occurs when onNewPart() is called with a SinglePartMIMEReader
//in the MultiPartMIMEReaderCallback. At this point the client has not committed
//to reading the part but has simply been notified that there is a part coming up.
//2. Inside of a part. If after being notified of a new part, the client then
//registers a IndividualPartReaderCallback with the SinglePartMIMEReader, they have then
//committed to consuming this part. Subsequently they can then call readPartData()
//on the SinglePartMIMEReader and then be notified when data is available via
//onPartDataAvailable().

public class MultiPartMIMEReader {

    //Hide the reader
    private final R2MultiPartMIMEReader _reader;
    //Note that the reader callback will only be allowed to change if there is a downstream
    //writer that needs to take this stream over and read from it.
    private volatile MultiPartMIMEReaderCallback _clientCallback;
    private final EntityStream _entityStream;
    private volatile String _preamble;

    class R2MultiPartMIMEReader implements Reader {
        private volatile ReadHandle _rh;
        private volatile List<Byte> _byteBuffer = new ArrayList<Byte>();
        //The reason for the first boundary vs normal boundary difference is because the first boundary MAY be missing the
        //leading CRLF.
        //No even though it is incorrect for a client to send a multipart/mime in this manner,
        //the RFC states that readers should be tolerant and be able to handle such cases.
        private final String _firstBoundary;
        private final String _normalBoundary;
        private final String _finishingBoundary;
        private final List<Byte> _firstBoundaryBytes = new ArrayList<Byte>();
        private final List<Byte> _normalBoundaryBytes = new ArrayList<Byte>();
        private final List<Byte> _finishingBoundaryBytes = new ArrayList<Byte>();
        private volatile ReadState _readState;
        private volatile SinglePartMIMEReader _currentSinglePartMIMEReader;

        //A signal from the R2 reader has been notified that all data is done being sent over. This does NOT mean that our
        //top level reader can be notified that they are done since data could still be in the buffer.
        private boolean _r2Done = false;

        //These are needed to support our iterative invocation of callbacks so that we don't end up with a recursive loop
        //which would lead to a stack overflow.
        private final Queue<Callable> _callbackQueue = new LinkedDeque<Callable>();
        private volatile boolean _callbackInProgress = false;

        private void processAndInvokeCallableQueue() {

            //This variable indicates that there is no current iterative invocation taking place. We can start one here.
            _callbackInProgress = true;

            while (!_callbackQueue.isEmpty()) {
                final Callable<Void> callable = _callbackQueue.poll();
                try {
                    callable.call();
                } catch (Exception clientCallbackException) {
                    handleExceptions(clientCallbackException);
                }
            }
            _callbackInProgress = false;
        }

        void handleExceptions(final Throwable throwable) {
            //All exceptions caught here should put the reader in a non-usable state. Continuing from this point forward
            //is not feasible.
            //We also will cancel here and have R2 read and drop all bytes on the floor. Otherwise we are obliged to read
            //and drop all bytes on the floor. It does not make any sense to enter this obligation when we are in
            //a non-usable state.
            //Exceptions here are indicative that there was malformed data provided to the MultiPartMIMEReader
            //OR that the client APIs threw exceptions when their callbacks were invoked.
            //We will also invoke the appropriate callbacks here indicating there is an exception while reading.
            //It is the responsibility of the consumer of this library to catch these exceptions and return 4xx.
            _rh.cancel();
            _readState = ReadState.READER_DONE;
            _clientCallback.onStreamError(throwable);
            if (_currentSinglePartMIMEReader != null) {
                _currentSinglePartMIMEReader._readerState.set(SingleReaderState.FINISHED);
                _currentSinglePartMIMEReader._callback.onStreamError(throwable);
            }
        }

        @Override
        public void onInit(ReadHandle rh) {
            _rh = rh;
            //Start the reading process since the top level callback has been bound.
            //Note that we read ahead a bit here and we read only we need. This is the approach suggested by R2.
            _readState = ReadState.READING_PREAMBLE;
            _rh.request(1);
        }

        //Note that only one thread should ever be calling onDataAvailable() at any time.
        //R2 only ever calls us sequentially. Client API calls via the readers that call onDataAvailable()
        //will also forcefully be controlled in a sequential manner.
        //It is for this reason we don't have to synchronize any data used within this method.
        //Also note that all exceptions thrown by asynchronous notification of client callbacks will be given back to
        //them by calling them onStreamError().
        //Anything thrown by us will make it to R2. This should ideally never ever happen and if it happens its a bug.
        //Regardless, if this was the case and we throw to R2 then:
        //A. In case of the server, R2 send back a 500 internal server error.
        //B. In case of the client, R2 will close the connection.
        @Override
        public void onDataAvailable(ByteString data) {

            //todo we should not use sublist as it prevents GC of the parent list
            ///////////////////////////////////////////////////////////////////////////////////////////////////
            //Drop unnecessary bytes on the ground before we even try anything else.

            //1. Drop the epilogue on the ground. No need to read into our buffer.
            if (_readState == ReadState.READING_EPILOGUE) {
                if (_r2Done) {
                    //If r2 has already notified we are done, we can wrap up. There is no need to use our
                    //iterative technique to call this callback because a client cannot possibly invoke us again.
                    _readState = ReadState.READER_DONE;
                    try {
                        //This can throw so we need to notify the client that their APIs threw an exception when we invoked them.
                        MultiPartMIMEReader.this._clientCallback.onFinished();
                    } catch (Exception clientCallbackException) {
                        handleExceptions(clientCallbackException);
                    }
                    return;
                }
                //Otherwise r2 has not notified us that we are done. So we keep getting more bytes and dropping them.
                _rh.request(1);
                return;
            }

            //2. Drop bytes for a top level abort.
            if (_readState == ReadState.ABORTING) {
                if (_r2Done) {
                    //If r2 has already notified we are done, we can wrap up. No need to look at remaining bytes in buffer.
                    //Also there is no need to use our iterative technique to call this callback because a client cannot
                    //possibly invoke us again.
                    _readState = ReadState.READER_DONE;
                    try {
                        //This can throw so we need to notify the client that their APIs threw an exception when we invoked them.
                        MultiPartMIMEReader.this._clientCallback.onAbandoned();
                    } catch (Exception clientCallbackException) {
                        handleExceptions(clientCallbackException);
                    }
                    return;
                }
                //Otherwise we keep on chugging forward and dropping bytes.
                _rh.request(1);
                return;
            }

            ///////////////////////////////////////////////////////////////////////////////////////////////////
            //Read data into our local buffer for further processing. All subsequent operations require this.

            appendByteStringToBuffer(data);


            ///////////////////////////////////////////////////////////////////////////////////////////////////
            //Read the preamble in.
            if (_readState == ReadState.READING_PREAMBLE) {

                final int firstBoundaryLookup = Collections.indexOfSubList(_byteBuffer, _firstBoundaryBytes);
                final int lastBoundaryLookup = Collections.indexOfSubList(_byteBuffer, _finishingBoundaryBytes);
                //Before reading the preamble, check to see if this is an empty multipart mime envelope. This can be checked by
                //examining if the location of the first boundary matches the location of the finishing boundary.
                //We also have to take into consideration that first boundary doesn't including the leading CRLF so we subtract
                //the length of the CRLF when doing the comparison.
                //This means that the envelope looked like:
                //Content-Type: multipart/<subType>; boundary="someBoundary"
                //
                //--someBoundary--
                if (firstBoundaryLookup - MultiPartMIMEUtils.CRLF_BYTES.length == lastBoundaryLookup) {
                    //In such a case we need to let the client know that reading is complete since there were no parts.
                    //There is no need to use our iterative technique to call this callback because a
                    //client cannot possibly invoke us again.
                    try {
                        //This can throw so we need to notify the client that their APIs threw an exception when we invoked them.
                        MultiPartMIMEReader.this._clientCallback.onFinished();
                    } catch (Exception clientCallbackException) {
                        handleExceptions(clientCallbackException);
                    }
                    return;
                }

                //Otherwise proceed
                if (firstBoundaryLookup > -1) {
                    //The boundary has been found. Everything up until this point is the preamble.
                    final List<Byte> preambleBytes = _byteBuffer.subList(0, firstBoundaryLookup);
                    _preamble = new String(ArrayUtils.toPrimitive(preambleBytes.toArray(new Byte[0])));
                    _byteBuffer = _byteBuffer.subList(firstBoundaryLookup, _byteBuffer.size());
                    //We can now transition to normal reading.
                    _readState = ReadState.PART_READING;
                } else {
                    //The boundary has not been found in the buffer, so keep looking
                    if (_r2Done) {
                        //If this happens that means that there was a problem. This means that r2 has
                        //fully given us all of the stream and we haven't found the boundary.
                        handleExceptions(new IllegalMimeFormatException("Malformed multipart mime request. No boundary found!"));
                        return;
                    }
                    _rh.request(1);
                    return;
                }
            }


            ///////////////////////////////////////////////////////////////////////////////////////////////////
            //Verify we have enough data in our buffer before proceeding

            //We buffer forward a bit if we have is less then the finishing boundary size.
            //Since the buffer begins with the boundary we want to make sure that its not a
            //finishing boundary.
            if (_byteBuffer.size() < _finishingBoundaryBytes.size()) {

                //If this happens and r2 has not notified us that we are done, then this is a problem. This means that
                //r2 has already notified that we are done and we didn't see the finishing boundary.
                //This should never happen.
                if (_r2Done) {
                    //Notify the reader of the issue.
                    handleExceptions(new IllegalMimeFormatException("Malformed multipart mime request. Finishing boundary missing!"));
                    return;
                }

                //Otherwise we need to read in some more data.
                _rh.request(1);
                return;
            }


            ///////////////////////////////////////////////////////////////////////////////////////////////////
            //PART_READING represents normal part reading operation and is where most of the time will be spent.
            if (_readState == ReadState.PART_READING) {

                //The goal of the logic here is the following:
                //1. If the buffer does not start with the boundary, then we fully consume as much of the buffer as possible.
                //We notify clients of as much data we can drain. These can possibly be the last bits of data they need.
                //Subsequent invocations of requestPartData() would then lead to the buffer starting with the boundary.
                //2. Otherwise if the buffer does start with boundary then we wrap up the previous part and
                //begin the new one.

                //Another invariant to note is that the result of this logic below will result in ONE of the
                //following (assumingi there are no error conditions):
                //1. onPartDataAvailable()
                //OR
                //2. OnAbandoned() on SinglePartCallback followed by onNewPart() on MultiPartCallback
                //OR
                //3. OnFinished() on SinglePartCallback followed by onNewPart() on MultiPartCallback
                //OR
                //4. OnAbandoned() on SinglePartCallback followed by onFinished() on MultiPartCallback
                //OR
                //5. OnFinished() on SinglePartCallback followed by onFinished() on MultiPartCallback
                //
                //Note that onPartDataAvailable() and onNewPart() are never called one after another in the logic
                //below because upon invocation of these callbacks, clients may come back to us immediately
                //and it can potentially lead to very confusing states. Furthermore its also more intuitive
                //to answer each client's request with only one callback.


                //todo clean this up
                final int boundaryIndex;
                final int boundarySize;
                if (_currentSinglePartMIMEReader == null) {
                    boundaryIndex = Collections.indexOfSubList(_byteBuffer, _firstBoundaryBytes);
                    boundarySize = _firstBoundaryBytes.size();
                } else {
                    boundaryIndex = Collections.indexOfSubList(_byteBuffer, _normalBoundaryBytes);
                    boundarySize = _normalBoundaryBytes.size();
                }

                //Buffer does not begin with boundary.
                if (boundaryIndex != 0) {

                    //We buffer forward a bit if we have is less or equal to the finishing boundary size.
                    //We want atleast one byte more then what the size of the finishing boundary is.
                    //We know the buffer doesn't begin with a boundary, but the second byte
                    //in the buffer may be the beginning. Therefore we need to be guaranteed to have atleast
                    //one possible byte to write to the client.
                    if (_byteBuffer.size() <= _finishingBoundaryBytes.size()) {

                        //If this happens and r2 has not notified us that we are done, then this is a problem. This means that
                        //r2 has already notified that we are done and we didn't see the finishing boundary.
                        //This should never happen.
                        if (_r2Done) {
                            //Notify the reader of the issue.
                            handleExceptions(new IllegalMimeFormatException("Malformed multipart mime request. Finishing boundary missing!"));
                            return;
                        }

                        //Otherwise we need to read in some more data.
                        _rh.request(1);
                        return;
                    }

                    //Only proceed if the _currentSinglePartMIMEReader is not equal to null. It will only be null
                    //the very first time.
                    if (_currentSinglePartMIMEReader != null) {

                        //We only proceed forward if there is a reader ready.
                        //By ready we mean that:
                        //1. They are ready to receive requested data on their onPartDataAvailable() callback.
                        //or
                        //2. They have requested an abort and are waiting for it to finish.
                        //If the current single part reader is not ready, then we just return and move on (we already read into the buffer)
                        //since the single part reader can then drive the flow of future data.
                        //todo - I don't think this comment/statement is really required since these are guaranteed to be in one of these states at this point in time.

                        final SingleReaderState currentState = _currentSinglePartMIMEReader._readerState.get();

                        if (currentState == SingleReaderState.REQUESTED_DATA || currentState == SingleReaderState.REQUESTED_ABORT) {

                            //We take different action if there a boundary exists in the buffer.
                            if (boundaryIndex == -1) {

                                //Boundary doesn't exist here, so let's drain the buffer.
                                //Note that we can't fully drain the buffer because the end of the buffer may include the partial
                                //beginning of the next boundary or even the finishing boundary.
                                //Therefore we grab the whole buffer but we leave the last _finishingBoundaryBytes.size()-1 number of bytes.
                                final List<Byte> useableBytes = _byteBuffer.subList(0, _byteBuffer.size() - _finishingBoundaryBytes.size());
                                _byteBuffer = _byteBuffer.subList(_byteBuffer.size() - _finishingBoundaryBytes.size(), _byteBuffer.size());

                                if (currentState == SingleReaderState.REQUESTED_DATA) {
                                    //Grab a copy of the data beforehand. Otherwise an eager thread could call requestPartData() on the single
                                    //part reader mutating our byte buffer even before we have a chance to respond via onPartDataAvailable().
                                    final ByteString clientData = ByteString.copy(ArrayUtils.toPrimitive(useableBytes.toArray(new Byte[0])));
                                    //We must set this before we provide the data. Otherwise if the client immediately decides to requestPartData()
                                    //they will see an exception because we are still in REQUESTED_DATA.
                                    _currentSinglePartMIMEReader._readerState.set(SingleReaderState.READY);

                                    //_currentSinglePartMIMEReader._callback.onPartDataAvailable(clientData);
                                    final Callable<Void> onPartDataAvailableInvocation = new MimeReaderCallables.onPartDataCallable(_currentSinglePartMIMEReader._callback, clientData);

                                    //Queue up this operation
                                    _callbackQueue.add(onPartDataAvailableInvocation);

                                    //If the while loop before us is in progress, we just return
                                    if (_callbackInProgress) {
                                        //We return to unwind the stack. Any queued elements will be taken care of the by the while loop
                                        //before us.
                                        return;
                                    } else {
                                        processAndInvokeCallableQueue();
                                        //If invoking the callables resulting in things stopping, we will return anyway.

                                    }

                                    //The client single part reader can then drive forward themselves.
                                } else {
                                    //This is an abort operation, so we need to drop the bytes and keep moving forward.
                                    _rh.request(1);
                                    //No need to explicitly return here.
                                }

                            } else {

                                //Boundary is in buffer
                                final List<Byte> useableBytes = _byteBuffer.subList(0, boundaryIndex);
                                _byteBuffer = _byteBuffer.subList(boundaryIndex, _byteBuffer.size());

                                if (currentState == SingleReaderState.REQUESTED_DATA) {
                                    //Grab a copy of the data beforehand. Otherwise an eager thread could call requestPartData() on the single
                                    //part reader mutating our byte buffer even before we have a chance to respond via onPartDataAvailable().
                                    final ByteString clientData = ByteString.copy(ArrayUtils.toPrimitive(useableBytes.toArray(new Byte[0])));

                                    //We must set this before we provide the data. Otherwise if the client immediately decides to requestPartData()
                                    //they will see an exception because we are still in REQUESTED_DATA.
                                    _currentSinglePartMIMEReader._readerState.set(SingleReaderState.READY);

                                    //_currentSinglePartMIMEReader._callback.onPartDataAvailable(clientData);
                                    final Callable<Void> onPartDataAvailableInvocation = new MimeReaderCallables.onPartDataCallable(_currentSinglePartMIMEReader._callback, clientData);

                                    //Queue up this operation
                                    _callbackQueue.add(onPartDataAvailableInvocation);

                                    //If the while loop before us is in progress, we just return
                                    if (_callbackInProgress) {
                                        //We return to unwind the stack. Any queued elements will be taken care of the by the while loop
                                        //before us.
                                        return;
                                    } else {
                                        processAndInvokeCallableQueue();
                                        //If invoking the callables resulting in things stopping, we will return anyway.
                                    }
                                } else {
                                    //drop the bytes
                                }
                                //This part is finished. Subsequently when the client asks for more data our logic will below
                                //will now see that the buffer begins with the boundary. This will finish up this part
                                //and then make a new part.
                            }
                        }
                    }
                } else {
                    //The beginning of the buffer contains a boundary.

                    //Close the current single part reader (except if this is the first boundary)
                    if (_currentSinglePartMIMEReader != null) {

                        //If this was a single part reader waiting to be notified of an abort
                        if (_currentSinglePartMIMEReader._readerState.get() == SingleReaderState.REQUESTED_ABORT) {
                            //If they cared to be notified of the abandonment.
                            if (_currentSinglePartMIMEReader._callback != null) {

                                //We need to prevent the client from asking for more data because they are done.
                                _currentSinglePartMIMEReader._readerState.set(SingleReaderState.FINISHED);

                                //Note we do not need to use our iterative invocation technique here because
                                //the client can't request more data.
                                //Also it is important to note that we CANNOT use the iterative technique.
                                //This is because the iterative technique will not return back here (to this line of
                                //code) which does not guarantee us proceeding forward from here.
                                try {
                                    _currentSinglePartMIMEReader._callback.onAbandoned();
                                } catch (Exception clientCallbackException) {
                                    //This could throw so handle appropriately.
                                    handleExceptions(clientCallbackException);
                                }

                            } //else no notification will happen since there was no callback registered.
                        } else {

                            //This was a part that cared about its data. Let's finish him up.

                            //We need to prevent the client from asking for more data because they are done.
                            _currentSinglePartMIMEReader._readerState.set(SingleReaderState.FINISHED);

                            //Note we do not need to use our iterative invocation technique here because
                            //the client can't request more data.
                            //Also it is important to note that we CANNOT use the iterative technique.
                            //This is because the iterative technique will not return back here (to this line of
                            //code) which does not guarantee us proceeding forward from here.
                            try {
                                _currentSinglePartMIMEReader._callback.onFinished();
                            } catch (Exception clientCallbackException) {

                                //This could throw so handle appropriately.
                                handleExceptions(clientCallbackException);
                            }
                        }
                        _currentSinglePartMIMEReader = null;
                        //We will now move on to notify the reader of the next part
                    }


                    //Before continuing verify that this isn't the final boundary in front of us.
                    final int lastBoundaryLookup = Collections.indexOfSubList(_byteBuffer, _finishingBoundaryBytes);
                    if (lastBoundaryLookup == 0) {
                        _readState = ReadState.READING_EPILOGUE;
                        //If r2 has already notified we are done, we can wrap up. Note that there still may be bytes
                        //sitting in our byteBuffer that haven't been consumed. These bytes must be the epilogue
                        //bytes so we can safely ignore them.
                        if (_r2Done) {
                            _readState = ReadState.READER_DONE;
                            //There is no need to use our iterative technique to call this callback because a
                            //client cannot possibly invoke us again.
                            try {
                                //This can throw so we need to notify the client that their APIs threw an exception when we invoked them.
                                MultiPartMIMEReader.this._clientCallback.onFinished();
                            } catch (Exception clientCallbackException) {
                                handleExceptions(clientCallbackException);
                            }
                            return;
                        }
                        //Keep on reading bytes and dropping them.
                        _rh.request(1);
                        return;
                    }

                    //Now read in headers:
                    //Now read until we have all the headers. Headers may or may not exist. According to the RFC:
                    //If the headers do not exist, we will see two CRLFs one after another.
                    //If at least one header does exist, we will see the headers followed by two CRLFs
                    //Essentially we are looking for the first occurrence of two CRLFs after we see the boundary.

                    //We need to make sure we can look ahead a bit here first. The minimum size of the buffer must be
                    //the size of the normal boundary plus consecutive CRLFs. This would be the bare minimum as it conveys
                    //empty headers.
                    if ((boundarySize + MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST.size()) > _byteBuffer.size()) {
                        if (_r2Done) {
                            //If r2 has already notified we are done, then this is a problem. This means that
                            //we have the remainder of the stream in memory and we see a non-finishing boundary that terminates
                            //immediately without a CRLF_BYTES. This is a sign of a stream that was prematurely terminated.
                            //MultiPartMIMEReader.this._clientCallback.onStreamError();
                            handleExceptions(new IllegalMimeFormatException("Malformed multipart mime request. Premature"
                                    + " termination of multipart mime body due to a boundary without a subsequent consecutive CRLF."));
                            return;
                        }
                        _rh.request(1);
                        return;
                    }

                    //Now we will determine the existence of headers.
                    //In order to do this we construct a window to look into. We will look inside of the buffer starting at the
                    //end of the boundary until the end of the buffer.
                    final List<Byte> possibleHeaderArea = _byteBuffer.subList(boundarySize, _byteBuffer.size());

                    //Find the two consecutive CRLFs.
                    final int headerEnding =
                            Collections.indexOfSubList(possibleHeaderArea, MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST);
                    if (headerEnding == -1) {
                        if (_r2Done) {
                            //If r2 has already notified us we are done, then this is a problem. This means that we saw a
                            //a boundary followed by a potential header area. This header area does not contain
                            //two consecutive CRLF_BYTES characters. This is a malformed stream.
                            //MultiPartMIMEReader.this._clientCallback.onStreamError();
                            handleExceptions(new IllegalMimeFormatException(
                                    "Malformed multipart mime request. Premature termination of headers within a part."));
                            return;
                        }
                        //We need more data since the current buffer doesn't contain the CRLFs.
                        _rh.request(1);
                        return;
                    }

                    //At this point, headerEnding represents the location of the first occurrence of consecutive CRLFs.
                    //It is important to note that it is possible for a malformed stream to not end its headers with consecutive
                    //CRLFs. In such a case, everything up until the first occurrence of the consecutive CRLFs will be considered
                    //part of the header area.

                    //Let's make a window into the header area. Note that we need to include the trailing consecutive CRLF bytes
                    //because we need to verify if the header area is empty, meaning it contains only consecutive CRLF bytes.
                    final List<Byte> headerByteSubList = possibleHeaderArea.subList(0,
                            headerEnding + MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST.size());

                    final Map<String, String> headers;
                    if (headerByteSubList.equals(MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST)) {
                        //The region of bytes after the boundary is composed of two CRLFs. Therefore we have no headers.
                        headers = Collections.emptyMap();
                    } else {
                        headers = new HashMap<String, String>();

                        //We have headers, lets read them in - we search using a sliding window.

                        //Currently our buffer is sitting just past the end of the boundary. Beyond this boundary
                        //there should be a CRLF followed by the first header. We will verify that this is indeed a CRLF
                        //and then we will skip it below.
                        final List<Byte> leadingBytes = headerByteSubList.subList(0, MultiPartMIMEUtils.CRLF_BYTE_LIST.size());
                        if (!leadingBytes.equals(MultiPartMIMEUtils.CRLF_BYTE_LIST)) {
                            handleExceptions(
                                    new IllegalMimeFormatException("Malformed multipart mime request. Headers are improperly constructed."));
                            return;
                        }

                        //The sliding-window-header-split technique here works because we are essentially splitting the buffer
                        //by looking at occurrences of CRLF bytes. This is analogous to splitting a String in Java but instead
                        //we are splitting a byte array.

                        //We start at an offset of i and currentHeaderStart because we need to skip the first CRLF.
                        int currentHeaderStart = MultiPartMIMEUtils.CRLF_BYTE_LIST.size();
                        final StringBuffer runningFoldedHeader = new StringBuffer(); //For folded headers. See below for details.

                        //Note that the end of the buffer we are sliding through is composed of two consecutive CRLFs.
                        //Our sliding window algorithm here will NOT evaluate the very last CRLF bytes (which would otherwise
                        //erroneously result in an empty header).
                        for (int i = MultiPartMIMEUtils.CRLF_BYTE_LIST.size(); i < headerByteSubList.size() - MultiPartMIMEUtils.CRLF_BYTE_LIST.size(); i++) {
                            final List<Byte> currentWindow = headerByteSubList.subList(i, i + MultiPartMIMEUtils.CRLF_BYTE_LIST.size());
                            if (currentWindow.equals(MultiPartMIMEUtils.CRLF_BYTE_LIST)) {
                                final List<Byte> currentHeaderBytes = headerByteSubList.subList(currentHeaderStart, i);
                                //At this point we MAY have found the end of a header because the current window is a CRLF.
                                //This could POTENTIALLY mean that from currentHeaderStart until i is a header.

                                //However before we can reach this conclusion we must check for header folding. Header folding is described
                                //in RFC 822 which states that headers may take up multiple lines (therefore delimited by CRLFs).
                                //This rule only holds true if there is exactly one CRLF followed by atleast one LWSP (linear white space).
                                //A LWSP can be composed of multiple spaces, tabs or newlines. However most implementations only use
                                //spaces or tabs. Therefore our reading of folded headers will support only CRLFs followed by atleast one
                                //space or tab.
                                //Furthermore this syntax is deprecated so there is no need for us to formally support the RFC here as
                                //long as we cover interoperability with major libraries.

                                //Therefore we have two options here:
                                //1. If the character in front of us IS a tab or a white space, we must consider this the first part of a
                                //multi line header value. In such a case we have to keep going forward and append the current header value.
                                //2. Otherwise the character in front of us is NOT a tab or a white space. We can then consider the current
                                //header bytes to compose a header that fits on a single line.

                                final byte[] headerBytes = ArrayUtils.toPrimitive(currentHeaderBytes.toArray(new Byte[0]));
                                String header = new String(headerBytes);

                                if (headerByteSubList.get(i + MultiPartMIMEUtils.CRLF_BYTE_LIST.size()) == MultiPartMIMEUtils.SPACE_BYTE
                                        || headerByteSubList.get(i + MultiPartMIMEUtils.CRLF_BYTE_LIST.size()) == MultiPartMIMEUtils.TAB_BYTE) {
                                    //Append the running concatenation of the folded header. We need to preserve the original header so
                                    //we also include the CRLF. The subsequent LWSP(s) will be also preserved because we don't trim here.
                                    runningFoldedHeader.append(header + MultiPartMIMEUtils.CRLF_STRING);
                                } else {
                                    //This is a single line header OR we arrived at the last line of a folded header.
                                    if (runningFoldedHeader.length() != 0) {
                                        runningFoldedHeader.append(header);
                                        header = runningFoldedHeader.toString();
                                        runningFoldedHeader.setLength(0); //Clear the buffer for future folded headers in this part
                                    }

                                    //Note that according to the RFC that header values may contain semi colons but header names may not.
                                    //Therefore it is acceptable to split on semicolon here and derive the header name from 0 -> semicolonIndex.
                                    final int colonIndex = header.indexOf(":");
                                    if (colonIndex == -1) {
                                        handleExceptions(new IllegalMimeFormatException("Malformed multipart mime request. Individual headers are "
                                                + "improperly formatted"));
                                        return;
                                    }
                                    headers.put(header.substring(0, colonIndex).trim(), header.substring(colonIndex + 1, header.length()).trim());
                                }
                                currentHeaderStart = i + MultiPartMIMEUtils.CRLF_BYTE_LIST.size();
                            }
                        }
                    }

                    //At this point we have actual part data starting from headerEnding going forward
                    //which means we can dump everything else beforehand. We need to skip past the trailing consecutive CRLFs.
                    //todo make this logic cleaner
                    _byteBuffer = _byteBuffer.subList(boundarySize + headerEnding + MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST.size(),
                            _byteBuffer.size());

                    //Notify the callback that we have a new part
                    _currentSinglePartMIMEReader = new SinglePartMIMEReader(headers);

                    //_clientCallback.onNewPart(_currentSinglePartMIMEReader);
                    final Callable<Void> onNewPartInvocation =
                            new MimeReaderCallables.onNewPartCallable(_clientCallback, _currentSinglePartMIMEReader);

                    //Queue up this operation
                    _callbackQueue.add(onNewPartInvocation);

                    //If the while loop before us is in progress, we just return
                    if (_callbackInProgress) {
                        //We return to unwind the stack. Any queued elements will be taken care of the by the while loop
                        //before us.
                        return;
                    } else {
                        processAndInvokeCallableQueue();
                        //No need to explicitly return here even if this invocation results in an exception.
                    }

                }
            }
        }

        @Override
        public void onDone() {
            //Be careful, we still could have space left in our buffer
            _r2Done = true;
            //We need to trigger onDataAvailable() again with empty data because there is an outstanding request
            //to _rh.request(1)
            onDataAvailable(ByteString.empty());
        }

        @Override
        public void onError(Throwable e) {
            //R2 has informed us of an error. So we notify our readers and shut things down.
            handleExceptions(e);
        }

        private void appendByteStringToBuffer(final ByteString byteString) {
            final byte[] byteStringArray = byteString.copyBytes();
            for (final byte b : byteStringArray) {
                _byteBuffer.add(b);
            }
        }

        private R2MultiPartMIMEReader(final String boundary) {
            //The RFC states that the preceeding CRLF_BYTES is a part of the boundary
            _firstBoundary = "--" + boundary;
            _normalBoundary = MultiPartMIMEUtils.CRLF_STRING + "--" + boundary;
            _finishingBoundary = _normalBoundary + "--";

            //todo - safe to assume charset?
            for (final byte b : _firstBoundary.getBytes()) {
                _firstBoundaryBytes.add(b);
            }

            for (final byte b : _normalBoundary.getBytes()) {
                _normalBoundaryBytes.add(b);
            }

            for (final byte b : _finishingBoundary.getBytes()) {
                _finishingBoundaryBytes.add(b);
            }
        }
    }

    private enum ReadState {
        CREATED, //At the very beginning. Before the callback is even bound.
        READING_PREAMBLE, //When we have started to read the preamble in.
        PART_READING, //Normal operation. Most time should be spent in this state.
        READING_EPILOGUE, //Epilogue is being read.
        ABORTING, //Top level reader asked for an abort.
        READER_DONE //This happens after the r2 reader has been called onDone() AND after the local byte buffer is exhausted.
        // At this point we can tell the top level reader that things are all done.
    }

    //These factories are technically not thread safe because multiple threads could call these concurrently.
    //However this is very unlikely to happen so are not going to be worried about it here.
    public static MultiPartMIMEReader createAndAcquireStream(final StreamRequest request,
                                                             final MultiPartMIMEReaderCallback clientCallback) throws IllegalMimeFormatException {
        return new MultiPartMIMEReader(request, clientCallback);
    }

    public static MultiPartMIMEReader createAndAcquireStream(final StreamResponse response,
                                                             final MultiPartMIMEReaderCallback clientCallback) throws IllegalMimeFormatException {
        return new MultiPartMIMEReader(response, clientCallback);
    }

    public static MultiPartMIMEReader createAndAcquireStream(final StreamRequest request) throws IllegalMimeFormatException {
        return new MultiPartMIMEReader(request, null);
    }

    public static MultiPartMIMEReader createAndAcquireStream(final StreamResponse response) throws IllegalMimeFormatException {
        return new MultiPartMIMEReader(response, null);
    }

    private MultiPartMIMEReader(final StreamRequest request, final MultiPartMIMEReaderCallback clientCallback) throws IllegalMimeFormatException {

        final String contentTypeHeaderValue = request.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER);
        if (contentTypeHeaderValue == null) {
            throw new IllegalArgumentException("No Content-Type header in this request");
        }

        _reader = new R2MultiPartMIMEReader(MultiPartMIMEUtils.extractBoundary(contentTypeHeaderValue));
        _entityStream = request.getEntityStream();
        _reader._readState = ReadState.CREATED;
        if (clientCallback != null) {
            _clientCallback = clientCallback;
            _entityStream.setReader(_reader);
        }
    }

    private MultiPartMIMEReader(StreamResponse response, MultiPartMIMEReaderCallback clientCallback) throws IllegalMimeFormatException {

        final String contentTypeHeaderValue = response.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER);
        if (contentTypeHeaderValue == null) {
            throw new IllegalArgumentException("No Content-Type header in this response");
        }

        _reader = new R2MultiPartMIMEReader(MultiPartMIMEUtils.extractBoundary(contentTypeHeaderValue));
        _entityStream = response.getEntityStream();
        _reader._readState = ReadState.CREATED;
        if (clientCallback != null) {
            _clientCallback = clientCallback;
            _entityStream.setReader(_reader);
        }
    }


    public class SinglePartMIMEReader {

        private final Map<String, String> _headers;
        private volatile SinglePartMIMEReaderCallback _callback = null;
        private final R2MultiPartMIMEReader _r2MultiPartMIMEReader;
        private volatile AtomicReference<SingleReaderState> _readerState =
                new AtomicReference<SingleReaderState>(SingleReaderState.CREATED);

        //Only MultiPartMIMEReader should ever create an instance
        private SinglePartMIMEReader(Map<String, String> headers) {
            _r2MultiPartMIMEReader = MultiPartMIMEReader.this._reader;
            _headers = headers;
        }

        //This call commits and binds this callback to finishing this part. This can
        //only happen once per life of each SinglePartMIMEReader.
        //Meaning PartBindException will be thrown if there are attempts to mutate this callback
        //We synchronize here to prevent any race conditions.
        public void registerReaderCallback(SinglePartMIMEReaderCallback callback)
                throws PartBindException {

            //Due to the possibility of malicious race conditions caused by clients we lock here.
            synchronized (MultiPartMIMEReader.this) {
                if (_callback != null) {
                    throw new PartBindException();
                }
                _readerState.set(SingleReaderState.READY);
                _callback = callback;
            }
        }

        //Headers can be null/empty here if the part doesn't have any headers (since headers are not required according to RFC)
        public Map<String, String> getHeaders() {
            return _headers;
        }

        //Read bytes from this part and notify on callback.
        //1. This throws PartNotInitializedException if this API is called without init() performed
        //2. If this part is fully consumed, meaning onFinished() has been called,
        //then any subsequent calls to readPartData() will throw PartFinishedException
        //3. Since this is async and we do not allow request queueing, repetitive calls will
        //result in StreamBusyException
        //4. If the r2 reader is done, either through an error or a proper finish. Calls to
        //requestPartData() will throw StreamFinishedException.
        public void requestPartData()
                throws PartNotInitializedException, PartFinishedException, StreamBusyException, StreamFinishedException {

            if (_r2MultiPartMIMEReader._readState == ReadState.READER_DONE) {
                throw new StreamFinishedException();
            }

            //We use an AtomicReference here to update the value of the SingleReaderState.
            //This is because multiple threads could call requestPartData() at the same time
            //and through a race condition call onDataAvailable() at the same time.
            //This would destroy our invariant that only one thread will ever process onDataAvailable().

            if (_readerState.get() == SingleReaderState.CREATED) {
                throw new PartNotInitializedException();
            }

            if (_readerState.get() == SingleReaderState.FINISHED) {
                throw new PartFinishedException();
            }

            final boolean stateChange = _readerState.compareAndSet(SingleReaderState.READY, SingleReaderState.REQUESTED_DATA);

            if (stateChange == false) {
                //Race condition. Already busy fulfilling requests so we throw.
                throw new StreamBusyException();
            }

            //We have updated our desire to be notified of data. Now we signal the reader to refresh itself and forcing it
            //to read from the internal buffer as much as possible. We do this by notifying it of an empty ByteString.
            _r2MultiPartMIMEReader.onDataAvailable(ByteString.empty());

            //Note that there is no stack overflow here due to the iterative callback invocation technique we are using.
        }

        //Abandon the current part. We read up until the next part and drop all bytes we encounter.
        //Once abandonment is done we call onCurrentPartAbandoned() on the SinglePartMIMEReader callback if it exists.
        //This API can be called before the registerReaderCallback() call. In such cases, since there is no
        //callback invoked when the abandonment is finished, since no callback was registered.
        //1. If this part is finished, meaning onAbandoned() or onFinished() has already been called
        //already, then a call to abandonPart() will throw PartFinishedException.
        //2. Since this is async and we do not allow request queueing, repetitive calls will
        //result in StreamBusyException
        public void abandonPart()
                throws PartFinishedException, StreamBusyException, StreamFinishedException {

            if (_r2MultiPartMIMEReader._readState == ReadState.READER_DONE) {
                throw new StreamFinishedException();
            }

            if (_readerState.get() == SingleReaderState.FINISHED) {
                //We already finished. Either through normal reading or through an abort.
                throw new PartFinishedException();
            }

            if (_readerState.get() == SingleReaderState.REQUESTED_DATA
                    || _readerState.get() == SingleReaderState.REQUESTED_ABORT) {
                //Already busy fulfilling request.
                throw new StreamBusyException();
            }

            //There is a possibility here of a race condition where two threads who see different values for _readerState
            //make it through. Hence we synchronize here.
            synchronized (MultiPartMIMEReader.this) {
                if (_readerState.get() == SingleReaderState.REQUESTED_ABORT) {
                    throw new StreamBusyException();
                }
                //At this point we are either in READY or CREATED
                _readerState.set(SingleReaderState.REQUESTED_ABORT);
            }

            //We have updated our desire to be aborted. Now we signal the reader to refresh itself and forcing it
            //to read from the internal buffer as much as possible. We do this by notifying it of an empty ByteString.
            _r2MultiPartMIMEReader.onDataAvailable(ByteString.empty());
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////
        // Chaining interface implementation. Note that we can't implement MultiPartMIMEDataSource because
        // then the methods below would become public and we do not want this. There is no way around this
        // so therefore we don't implement the formal interface. Even if MultiPartMIMEDataSource was an
        // abstract class with public abstract methods its not possible to reduce the visibility
        // of these methods in subclasses.
        // The only suitable alternative is to create a wrapper class that hides the interface implementation
        // and delegates.
        private volatile MultiPartMIMEWriter.DataSourceHandleImpl _dataSourceHandle;

         void onInit(MultiPartMIMEWriter.DataSourceHandleImpl dataSourceHandle) {
            //We have been informed that this part will be treated as a data source by the MultiPartMIMEWriter.
            //So we will prepare for this task by:
            //1. Storing the handle to write data to.
            //2. Creating a callback to register ourselves with.
            _dataSourceHandle = dataSourceHandle;
            SinglePartMIMEReaderCallback singlePartMIMEChainReaderCallback = new SinglePartMIMEReaderDataSourceCallback(_dataSourceHandle);
            registerReaderCallback(singlePartMIMEChainReaderCallback);
        }

        void onWritePossible() {
            //When we are told to produce some data we will requestPartData() on ourselves which will
            //result in onPartDataAvailable() in SinglePartMIMEChainReaderCallback(). The result of that will write
            //data to the data source handle which will write it further down stream.
            requestPartData();
        }

        void onAbort(Throwable e) {
            //If we send a part off to someone else and it gets aborted when they are told to write
            //this indicates an error in reading.
            //Note that this could occur in one of two cases:
            //1. This was an individual part as a SinglePartMIMEReader specified to be sent out. In this case the application
            //developer can gracefully recover after being called onStreamError().
            //2. This was an individual part from a MultiPartMIMEReader data source specified to be sent out. In this case
            //the application developer has given up control on being able to handle this gracefully. In this case
            //onStreamError() will be called on MultiPartMIMEChainReaderCallback which will effectively shutdown the
            //MultiPartMIMEReader from reading. This behavior is similar to what would happen if there was an error reading
            //(i.e there was an invalid multipart mime body).

           MultiPartMIMEReader.this._clientCallback.onStreamError(e);
        }

        Map<String, String> dataSourceHeaders() {
            return _headers;
        }

    }

    static class SinglePartMIMEReaderDataSource implements MultiPartMIMEDataSource
    {
        private final SinglePartMIMEReader _singlePartMIMEReader;

        SinglePartMIMEReaderDataSource(final SinglePartMIMEReader singlePartMIMEReader) {
            _singlePartMIMEReader = singlePartMIMEReader;
        }

        @Override
        public void onInit(MultiPartMIMEWriter.DataSourceHandleImpl dataSourceHandle) {
            _singlePartMIMEReader.onInit(dataSourceHandle);
        }

        @Override
        public void onWritePossible() {
            _singlePartMIMEReader.onWritePossible();
        }

        @Override
        public void onAbort(Throwable e) {
            _singlePartMIMEReader.onAbort(e);
        }

        @Override
        public Map<String, String> dataSourceHeaders() {
            return _singlePartMIMEReader.dataSourceHeaders();
        }
    }

    private enum SingleReaderState {
        CREATED, //Initial construction
        READY, //Callback has been bound
        REQUESTED_DATA, //Requested data, waiting to be notified
        REQUESTED_ABORT, //Waiting for an abort to finish
        FINISHED //This callback is done.
    }

    public boolean haveAllPartsFinished() {
        return _reader._readState == ReadState.READER_DONE;
    }

    //Simlar to javax.mail we only allow clients to get ahold of the preamble
    public String getPreamble() {
        return _preamble;
    }

    //Reads through and abandons the new part and additionally the whole stream.
    //This can ONLY be called if there is no part being actively read, meaning that
    //the current SinglePartMIMEReader has not been initialized with an SinglePartMIMEReaderCallback.
    //If this is violated we throw a StreamBusyException.
    //1. Once the stream is finished being abandoned, we call allPartsAbandoned().
    //2. If the stream is finished, subsequent calls will throw StreamFinishedException
    //3. Since this is async and we do not allow request queueing, repetitive calls will
    //result in StreamBusyException.
    //4. If this MultiPartMIMEReader was created without a callback, and none has been registered yet
    //then a call to abanonAllParts() will not notify on completion.
    void abandonAllParts()
            throws StreamBusyException, StreamFinishedException, ReaderNotInitializedException {

        synchronized (this) {
            //No callback registered. This is a problem.
            if (_clientCallback == null) {
                throw new ReaderNotInitializedException();
            }

            //We are already done or almost done.
            if (_reader._readState == ReadState.READER_DONE || _reader._readState == ReadState.READING_EPILOGUE) {
                throw new StreamFinishedException();
            }

            if (_reader._readState == ReadState.READING_PREAMBLE) {
                throw new StreamBusyException(); //We are not in a position to transfer
                //to a new callback yet.
            }

            if (_reader._readState == ReadState.ABORTING) {
                throw new StreamBusyException(); //Not allowed
            }

            if (_reader._currentSinglePartMIMEReader._readerState.get() != SingleReaderState.CREATED) {
                throw new StreamBusyException(); //Can't transition at this point in time
            }
        }

        _reader._readState = ReadState.ABORTING;
        _reader.onDataAvailable(ByteString.empty());
    }

    //This can ONLY be called if there is no part being actively read, meaning that
    //the current SinglePartMIMEReader has had no callback registered. Violation of this
    //will throw StreamBusyException.
    //This can be set even if no parts in the stream have actually been consumed, i.e
    //after the very first invocation of onNewPart() on the initial MultiPartMIMEReaderCallback.
    public void registerReaderCallback(MultiPartMIMEReaderCallback clientCallback)
            throws StreamFinishedException, StreamBusyException {

        synchronized (this) {
            //This callback can only be registered if we are in ReadState.CREATED meaning that no callback has been bound yet.
            //Or we are in the middle of reading data between parts which is ReadState.PART_READING.

            //First we throw exceptions for all _reader states where it is incorrect to transfer callbacks.
            //We have to handle all the individual incorrect states one by one so that we can that we can throw
            //fine grain exceptions:
            if (_reader._readState == ReadState.READER_DONE || _reader._readState == ReadState.READING_EPILOGUE) {
                throw new StreamFinishedException(); //We are already done or almost done.
            }

            if (_reader._readState == ReadState.READING_PREAMBLE) {
                //This would happen if a client registers a callback multiple times
                //immediately.
                throw new StreamBusyException(); //We are not in a position to transfer
                //to a new callback yet.
            }

            if (_reader._readState == ReadState.ABORTING) {
                throw new StreamBusyException(); //Not allowed
            }

            //Now we verify that single part reader is in the correct state.
            //The first time the callback is registered, _currentSinglePartMIMEReader will be null which is fine.
            //Subsequent calls will verify that the _currentSinglePartMIMEReader is in the desired state.
            if (_reader._currentSinglePartMIMEReader != null &&
                    _reader._currentSinglePartMIMEReader._readerState.get() != SingleReaderState.CREATED) {
                throw new StreamBusyException(); //Can't transition at this point in time
            }

            if (_clientCallback == null) {
                //This is the first time it's being set
                _clientCallback = clientCallback;
                _entityStream.setReader(_reader);
                return; //The lock will be released normally here.
            } else {
                //This is just a transfer to a new callback.
                _clientCallback = clientCallback;
            }
        }

        //Start off the new client callback. If there was already a _currentSinglePartMIMEReader, we get them started off
        //on that. Otherwise if this is the first time then we can just return from here and let the fact we setReader()
        //above drive the first invocation of onNewPart().
        //Note that it is not possible for _currentSinglePartMIMEReader to be null at ANY time except the very first time
        //registerReaderCallback() is invoked.

        if (_reader._currentSinglePartMIMEReader != null) {
            //Also note that if the client is really abusive it is possible for them
            //to call registerReaderCallback() over and over again which would lead to a stack overflow.
            //However this is clearly a client bug so we will not account for it here.
            _clientCallback.onNewPart(_reader._currentSinglePartMIMEReader);
        }
    }

    R2MultiPartMIMEReader getR2MultiPartMIMEReader() {
        return _reader;
    }

}
