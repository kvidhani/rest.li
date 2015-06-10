package com.linkedin.multipart.reader;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.MultiPartMIMEUtils;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.ArrayUtils;


/**
 * Created by kvidhani on 5/18/15.
 */
public class MultiPartMIMEReader {

  //Hide the reader
  private final R2MultiPartMimeReader _reader;
  //Note that the reader callback will only be allowed to change if there is a downstream
  //writer that needs to take this stream over and read from it.
  private MultiPartMIMEReaderCallback _clientCallback;
  private final EntityStream _entityStream;
  private String _preamble;

  private class R2MultiPartMimeReader implements Reader {
    private ReadHandle _rh;
    private List<Byte> _byteBuffer = new ArrayList<Byte>();

    private final String _boundary;
    private final String _finishingBoundary;
    private final List<Byte> _boundaryBytes = new ArrayList<Byte>();
    private final List<Byte> _finishingBoundaryBytes = new ArrayList<Byte>();

    private ReadState _readState = ReadState.READING_PREAMBLE;

    private SinglePartMIMEReader _currentSinglePartMIMEReader;

    @Override
    public void onInit(ReadHandle rh) {
      _rh = rh;
      //Start the reading process since the top level callback has been bound.
      //Note that we read ahead a bit here
      _rh.request(5);
    }

    //todo consider malformed bodies of all sorts! premature onDone()? You bet!
    //todo consider no parts, or just one part, or even just one tiny part that is empty
    //todo max header limit - open JIRA
    //todo when using sublist, keep in mind that it will prevent garbage collection of parent list
    //todo consider replacing all of this List <Byte> and indexOfSublist usage since its n^2
    //todo what happens if there was no data to begin with at all?
    //todo handle illicit or incomplete multipart mime requests
    //todo any multithreaded considerations? Remember to use volatiles in certain cases!

    @Override
    public void onDataAvailable(ByteString data) {

      //No need to check for ReadState.DONE since if this was the case, onDataAvailable() wouldn't even be called

      //Drop the epilogue on the ground.
      if (_readState == ReadState.READING_EPILOGUE) {
        _rh.request(1);
        return;
      }

      ///////////////////////////////////////////////////////////////////////////////////////////////////
      //Read data into our local buffer for further processing.

      //We buffer forward a bit if we are out of data or if the data we have is less then the finishing boundary size.
      //This is so that:
      //1. We don't confuse a middle boundary vs the end boundary. We need to look forward as large as the bytes in _finishingBoundaryBytes.
      //2. Also to cover the case where a client asks for data but we have ONLY the boundary in buffer
      //which would then lead us to giving the client empty data when we call onPartDataAvailable().
      if (_byteBuffer.size() < _finishingBoundaryBytes.size()) {
        _rh.request(1);
        return;
      }

      //All operations will require us to buffer
      appendByteStringToBuffer(data);

      ///////////////////////////////////////////////////////////////////////////////////////////////////
      //Read the preamble in.
      if (_readState == ReadState.READING_PREAMBLE) {

        int tempLookup = Collections.indexOfSubList(_byteBuffer, _boundaryBytes);
        if (tempLookup > -1) {
          //The boundary has been found. Everything up until this point is the preamble.
          final List<Byte> preambleBytes = _byteBuffer.subList(0, tempLookup);
          _preamble = new String(ArrayUtils.toPrimitive((Byte[]) preambleBytes.toArray()));
          _byteBuffer = _byteBuffer.subList(tempLookup, _byteBuffer.size());
          //We can now transition to normal reading.
          _readState = ReadState.PART_READING;
        } else {
          //The boundary has not been found in the buffer, so keep looking
          _rh.request(1);
          return;
        }
      }

      ///////////////////////////////////////////////////////////////////////////////////////////////////
      //Determine if we are now at the ending boundary. The only way we take action is if the ending boundary
      //is at the beginning of the buffer. Otherwise there is still data to be processed.
      //Our read logic further will always force this if statement to be executed at the end of the stream.
      if (Collections.indexOfSubList(_byteBuffer, _finishingBoundaryBytes) == 0) {
        _readState = ReadState.READING_EPILOGUE;
        //Keep on reading bytes and dropping them.
        _rh.request(1);
        return;
      }

      ///////////////////////////////////////////////////////////////////////////////////////////////////
      //PART_READING represents normal operation.
      if (_readState == ReadState.PART_READING) {

        final int boundaryIndex = Collections.indexOfSubList(_byteBuffer, _boundaryBytes);

        if (boundaryIndex == 0) { //buffer begins with boundary

          //Close the current single part reader (except if this is the first boundary)
          if (_currentSinglePartMIMEReader != null) {

            //If this was a single part reader waiting to be notified of an abort
            if(_currentSinglePartMIMEReader._readerState == SingleReaderState.REQUESTED_ABORT) {
              //If they cared to be notified of the abandonment.
              if(_currentSinglePartMIMEReader._callback!= null) {
                _currentSinglePartMIMEReader._callback.onAbandoned();
              }//else no notification will happen
            } else {
              //This was a part that cared about its data. Let's finish him up.
              //The order here matters. We must set isFinished to true first. Otherwise if we call onFinished() first
              //a poor client may immediately ask for more data and we would try to service this data since we don't
              //know that this part is finished.
              _currentSinglePartMIMEReader._readerState = SingleReaderState.FINISHED;
              _currentSinglePartMIMEReader._callback.onFinished();
              //We need to null the single part reader out so that we don't call onFinished() multiple times.
              _currentSinglePartMIMEReader = null;
              //We will now move on to notify the reader of the next part
            }
          }

          //Now read until we have all the headers. Headers may or may not exist. According to the RFC:
          //If the headers do not exist, we will see two CRLFs one after another.
          //If at least one header does exist, we will see the headers followed by two CRLFs
          //Essentially we are looking for the first occurrence of two CRLFs after we see the boundary.

          //We need to make sure we can look ahead a bit here first
          final int boundaryEnding = boundaryIndex + _boundaryBytes.size();
          if ((boundaryEnding + MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST.size()) > _byteBuffer.size()) {
            _rh.request(1);
            return;
          }

          //Now determine the existence of headers. We look inside of the buffer starting at the end of the boundary
          //until the end of the buffer.
          final List<Byte> possibleHeaderArea = _byteBuffer.subList(boundaryEnding, _byteBuffer.size());
          //Find the two consecutive CRLFs.
          final int headerEnding =
              Collections.indexOfSubList(possibleHeaderArea, MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST);
          if (headerEnding == -1) {
            //We need more data since the current buffer doesn't contain the CRLFs.
            _rh.request(1);
            return;
          }

          //Now we found the end. Let's make a window into the header area.
          final List<Byte> headerByteSubList = _byteBuffer.subList(boundaryEnding, headerEnding);

          final Map<String, String> headers;
          if (headerByteSubList.equals(MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST)) {
            //The region of bytes after the the two CRLFs is empty. Therefore we have no headers.
            headers = Collections.emptyMap();
          } else {
            headers = new HashMap<String, String>();
            //We have headers, lets read them in - we search using a sliding window.
            int currentHeaderStart = 0;
            for (int i = 0; i < headerByteSubList.size() - MultiPartMIMEUtils.CRLF_BYTE_LIST.size(); i++) {
              final List<Byte> currentWindow = headerByteSubList.subList(i, MultiPartMIMEUtils.CRLF_BYTE_LIST.size());
              if (currentWindow.equals(MultiPartMIMEUtils.CRLF_BYTE_LIST)) {
                //We found the end of a header. This means that from currentHeaderStart until i we have a header
                final List<Byte> currentHeaderBytes = headerByteSubList.subList(currentHeaderStart, i);
                final byte[] headerBytes = ArrayUtils.toPrimitive((Byte[]) currentHeaderBytes.toArray());
                final String header = new String(headerBytes);
                final int colonIndex = header.indexOf(":");
                headers.put(header.substring(0, colonIndex), header.substring(colonIndex, header.length()));
                currentHeaderStart = i + MultiPartMIMEUtils.CRLF_BYTE_LIST.size();
              }
            }
          }

          //At this point we have actual part data starting from headerEnding going forward
          //which means we can dump everything else beforehand.
          _byteBuffer = _byteBuffer.subList(headerEnding, _byteBuffer.size());

          //Notify the callback that we have a new part
          _currentSinglePartMIMEReader = new SinglePartMIMEReader(headers);
          _clientCallback.onNewPart(_currentSinglePartMIMEReader);
          return;
        } else { //Buffer does not begin with boundary.

          //We only proceed forward if there is a reader ready:
          //By ready we mean that:
          //1. They are ready to receive requested data on their onPartDataAvailable() callback.
          //or
          //2. They have requested an abort and are waiting for it to finish.
          //If the current single part reader is not ready, then we just return and move on (we already read into the buffer)
          //since the single part reader can then drive the flow of future data.
          final SingleReaderState currentState = _currentSinglePartMIMEReader._readerState;
          if (currentState == SingleReaderState.REQUESTED_DATA || currentState == SingleReaderState.REQUESTED_ABORT) {
            if (boundaryIndex > -1) {
              //Boundary is in buffer
              final List<Byte> useableBytes = _byteBuffer.subList(0, boundaryIndex);
              _byteBuffer = _byteBuffer.subList(boundaryIndex, _byteBuffer.size());

              if(currentState == SingleReaderState.REQUESTED_DATA) {
                //Grab a copy of the data beforehand. Otherwise an eager thread could call requestPartData() on the single
                //part reader mutating our byte buffer even before we have a chance to respond via onPartDataAvailable().
                final ByteString clientData = ByteString.copy(ArrayUtils.toPrimitive((Byte[]) useableBytes.toArray()));
                //We must set this before we provide the data. Otherwise if the client immediately decides to requestPartData()
                //they will see an exception because we are still in REQUESTED_DATA.
                _currentSinglePartMIMEReader._readerState = SingleReaderState.READY;
                _currentSinglePartMIMEReader._callback.onPartDataAvailable(clientData);
              } else {
                //drop the bytes
              }
              //This part is finished. We we request more data. This will result in a new call to onDataAvailable()
              //which will see that the buffer begins with the boundary. This will finish up this part and then
              //make a new part.
              _rh.request(1);

            } else {
              //Boundary doesn't exist here, so let's drain the buffer.
              //Note that we can't fully drain the buffer because the end of the buffer may include the partial
              //beginning of the boundary or even the finishing boundary.
              //Therefore we grab the whole buffer but we leave the last _finishingBoundaryBytes.size() number of bytes.
              //This is so that we are guaranteed that future appends to the _byteBuffer will result in at least one
              //byte available for further processing before the boundary is reached.
              final List<Byte> useableBytes =
                  _byteBuffer.subList(0, _byteBuffer.size() - _finishingBoundaryBytes.size());
              _byteBuffer =
                  _byteBuffer.subList(_byteBuffer.size() - _finishingBoundaryBytes.size(), _byteBuffer.size());

              if(currentState == SingleReaderState.REQUESTED_DATA) {
                //Grab a copy of the data beforehand. Otherwise an eager thread could call requestPartData() on the single
                //part reader mutating our byte buffer even before we have a chance to respond via onPartDataAvailable().
                final ByteString clientData = ByteString.copy(ArrayUtils.toPrimitive((Byte[]) useableBytes.toArray()));
                //We must set this before we provide the data. Otherwise if the client immediately decides to requestPartData()
                //they will see an exception because we are still in REQUESTED_DATA.
                _currentSinglePartMIMEReader._readerState = SingleReaderState.READY;
                _currentSinglePartMIMEReader._callback.onPartDataAvailable(clientData);
                //The client single part reader can then drive forward themselves.
              } else {
                //drop the bytes and keep moving forward
                _rh.request(1);
              }
            }
          }
          return;
        }
      }
    }

    @Override
    public void onDone() {
      //Be careful, we still could have space left in our buffer
      _readState = ReadState.R2_DONE;
      adkhfad todo finisht his
      MultiPartMIMEReader.this._clientCallback.onFinished();
    }

    @Override
    public void onError(Throwable e) {
      //todo error cases
      //_clientCallback.onStreamError(e);
      //_readState = ReadState.DONE;
    }

    private void appendByteStringToBuffer(final ByteString byteString) {
      final byte[] byteStringArray = byteString.copyBytes();
      for (final byte b : byteStringArray) {
        _byteBuffer.add(b);
      }
    }

    private R2MultiPartMimeReader(final String boundary) {
      //The RFC states that the preceeding CRLF is a part of the boundary
      _boundary = MultiPartMIMEUtils.CRLF + "--" + boundary;
      _finishingBoundary = _boundary + "--";

      for (final byte b : _boundary.getBytes()) {
        _boundaryBytes.add(b); //safe to assume charset?
      }

      for (final byte b : _finishingBoundary.getBytes()) {
        _finishingBoundaryBytes.add(b); //safe to assume charset?
      }
    }
  }

  private enum ReadState {
    READING_PREAMBLE, //At the beginning. When we have to read the preamble in.
    PART_READING, //Normal operation. Most time should be spent in this state.
    READING_EPILOGUE, //Epilogue is being read.
    R2_DONE, //R2 reader has been notified that all data is done being sent over. This does NOT mean that our top level
    //reader can be notified that they are done since data could still be in the buffer.
    MIME_READER_DONE //This happens after R2_DONE and after the local byte buffer is exhausted. At this point we
    //can tell the top level reader that things are all done.
  }

  public static MultiPartMIMEReader createAndAcquireStream(final StreamRequest request,
      final MultiPartMIMEReaderCallback clientCallback) {
    return new MultiPartMIMEReader(request, clientCallback);
  }

  public static MultiPartMIMEReader createAndAcquireStream(final StreamResponse response,
      final MultiPartMIMEReaderCallback clientCallback) {
    return new MultiPartMIMEReader(response, clientCallback);
  }

  public static MultiPartMIMEReader createAndAcquireStream(final StreamRequest request) {
    return new MultiPartMIMEReader(request, null);
  }

  public static MultiPartMIMEReader createAndAcquireStream(final StreamResponse response) {
    return new MultiPartMIMEReader(response, null);
  }

  private MultiPartMIMEReader(final StreamRequest request, final MultiPartMIMEReaderCallback clientCallback) {

    final String contentTypeHeaderValue = request.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER);
    if (contentTypeHeaderValue == null) {
      throw new IllegalArgumentException("No Content-Type header in this request");
    }

    _reader = new R2MultiPartMimeReader(MultiPartMIMEUtils.extractBoundary(contentTypeHeaderValue));
    _entityStream = request.getEntityStream();
    if (clientCallback != null) {
      _clientCallback = clientCallback;
      _entityStream.setReader(_reader);
    }
  }

  private MultiPartMIMEReader(StreamResponse response, MultiPartMIMEReaderCallback clientCallback) {

    final String contentTypeHeaderValue = response.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER);
    if (contentTypeHeaderValue == null) {
      throw new IllegalArgumentException("No Content-Type header in this response");
    }

    _reader = new R2MultiPartMimeReader(MultiPartMIMEUtils.extractBoundary(contentTypeHeaderValue));
    _entityStream = response.getEntityStream();
    if (clientCallback != null) {
      _clientCallback = clientCallback;
      _entityStream.setReader(_reader);
    }
  }

  public class SinglePartMIMEReader {

    private final Map<String, String> _headers;
    private volatile SinglePartMIMEReaderCallback _callback = null;
    private final R2MultiPartMimeReader _r2MultiPartMimeReader;
    private volatile AtomicReference<SingleReaderState> _readerState;

    //Only MultiPartMIMEReader should ever create an instance
    private SinglePartMIMEReader(Map<String, String> headers) {
      _r2MultiPartMimeReader = MultiPartMIMEReader.this._reader;
      _headers = headers;
      _readerState.set(SingleReaderState.CREATED);
    }

    //This call commits and binds this callback to finishing this part. This can
    //only happen once per life of each SinglePartMIMEReader.
    //Meaning PartBindException will be thrown if there are attempts to mutate this callback
    //We synchronize here to prevent any race conditions.
    public synchronized void registerReaderCallback(SinglePartMIMEReaderCallback callback) throws PartBindException {
      if(_callback != null) {
        throw new PartBindException();
      }
      _callback = callback;
    }

    //Headers can be null/empty here if the part doesn't have any headers (since headers are not required)
    public Map<String, String> getHeaders() {
      return _headers;
    }

    //Read bytes from this part and notify on callback.
    //1. This throws PartNotInitializedException if this API is called without init() performed
    //2. If this part is fully consumed, meaning onFinished() has been called,
    //then any subsequent calls to readPartData() will throw PartFinishedException
    //3. Since this is async and we do not allow request queueing, repetitive calls will
    //result in StreamBusyException
    //todo - consider allowing multiple parts
    //todo - We would then have to serially provide all of them one after another
    public void requestPartData()
        throws PartNotInitializedException, PartFinishedException, StreamBusyException {

      //We use an AtomicReference here to update the value of the SingleReaderState.
      //This is because multiple threads could call requestPartData() at the same time
      //and through a race condition call onDataAvailable() at the same time.
      //This would destroy our invariant that only one thread will ever process onDataAvailable().
      //todo mention this in comments above onDataAvailable

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
      _r2MultiPartMimeReader.onDataAvailable(ByteString.empty());

      //Note that there is no stack overflow here. Let us assume the following happens:
      //This thread which called requestPartData() will call onDataAvailable() as shown above.
      //Now the logic in onDataAvailable() will read a good chunk of data out and call onPartDataAvailable().
      //Now let us assume that the same thread now from inside of onPartDataAvailable() calls requestPartData().
      //At this point the cycle has to stop because the buffer inside of our private r2 reader has gotten sufficiently
      //small. At this point the r2 reader will request for more data from r2 using _rh.request() followed by a return.
      //This will result in this reset of this thread's stack frame.
    }

    //Abandon the current part.
    //We read up until the next part and drop all bytes we encounter.
    //Once abandonment is done we call onCurrentPartAbandoned() on the
    //SinglePartMIMEReader callback.
    //This API can be called before the init() call. In such cases, since there is no
    //callback invoked when the abandonment is finished, since no callback was registered.
    //1. If this part is finished, meaning onAbandoned() or
    //onFinished() has already been called
    //already, then a call to abandonPart() will throw PartFinishedException
    //2. Since this is async and we do not allow request queueing, repetitive calls will
    //result in StreamBusyException
    public void abandonPart() throws PartFinishedException, StreamBusyException {

      //karim resume
      //todo synchronize? here
      //fix onDone bug
      //finish abort
      //finish throwables
      //finisih edge cases
      //then do chaining
      //then write few tests
      //then rb
      if (_readerState == SingleReaderState.FINISHED) {
        //We already finished. Either through normal reading or through an abort.
        throw new PartFinishedException();
      }

      if (_readerState == SingleReaderState.REQUESTED_DATA || _readerState == SingleReaderState.REQUESTED_ABORT) {
        //Already busy fulfilling request.
        throw new StreamBusyException();
      }

      //At this point we are guaranteed that we are in READY or we are in CREATED
      _readerState = SingleReaderState.REQUESTED_ABORT;

      //We have updated our desire to be aborted. Now we signal the reader to refresh itself and forcing it
      //to read from the internal buffer as much as possible. We do this by notifying it of an empty ByteString.
      _r2MultiPartMimeReader.onDataAvailable(ByteString.empty());

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
    return _reader._readState == ReadState.MIME_READER_DONE;
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
  //then a call to abanonAllParts() will result in a ReaderNotInitializedException.
  public void abandonAllParts()
      throws StreamBusyException, StreamFinishedException, ReaderNotInitializedException {

    //todo this done thing is not correctg
    //if(_reader._readState == ReadState.DONE) {
    //  throw new StreamFinishedException(); //We are already done.
   // }

    //todo
  }

  //Package Private reader callback registration
  //Used ONLY by a MultiPartMIMEWriter when they want to take over the rest of this stream.
  //This can ONLY be called if there is no part being actively read, meaning that
  //the current SinglePartMIMEReader has had no callback registered. Violation of this
  //will throw StreamBusyException.
  //This can be set even if no parts in the stream have actually been consumed, i.e
  //after the very first invocation of onNewPart() on the initial MultiPartMIMEReaderCallback.
  void registerReaderCallback(MultiPartMIMEReaderCallback clientCallback) throws StreamFinishedException,
      StreamBusyException {
    //if(_reader._readState == ReadState.DONE) {
    //  throw new StreamFinishedException(); //We are already done.
    //}
    //todo
    //if(_reader._currentSinglePartMIMEReader._readerState!=)
    _clientCallback = clientCallback;
    _entityStream.setReader(_reader);
  }
}
