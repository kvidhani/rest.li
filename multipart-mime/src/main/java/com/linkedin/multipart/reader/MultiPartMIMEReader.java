package com.linkedin.multipart.reader;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.MultiPartMIMEUtils;
import com.linkedin.multipart.reader.exceptions.PartBindException;
import com.linkedin.multipart.reader.exceptions.PartFinishedException;
import com.linkedin.multipart.reader.exceptions.PartNotInitializedException;
import com.linkedin.multipart.reader.exceptions.ReaderNotInitializedException;
import com.linkedin.multipart.reader.exceptions.StreamBusyException;
import com.linkedin.multipart.reader.exceptions.StreamFinishedException;
import com.linkedin.multipart.writer.DataSourceHandle;
import com.linkedin.multipart.writer.MultiPartMIMEDataSource;
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
  private ByteString _currentBuffer; //We will always be a little ahead
  private final EntityStream _entityStream;

  private class R2MultiPartMimeReader implements Reader {
    private ReadHandle _rh;
    private List<Byte> _byteList = new ArrayList<Byte>();
    private String _preamble;

    private final String _boundary;
    private final String _finishingBoundary;
    private final List<Byte> _boundaryBytes = new ArrayList<Byte>();
    private final List<Byte> _finishingBoundaryBytes = new ArrayList<Byte>();

    private ReadState _readState = ReadState.READING_PREAMBLE;
    private SinglePartMIMEReader _currentSinglePartMIMEReader;
    private boolean _readerReady;

    @Override
    public void onInit(ReadHandle rh) {
      _rh = rh;
      //start the reading process since the top level callback has been bound
      //todo figure out how to tune this and what makes sense
      _rh.request(5);
    }

    private void signalR2Reader() {
      if(_currentSinglePartMIMEReader._isFinished) {
        _currentSinglePartMIMEReader._callback.onFinished();
      }

      //Drive data with a refresh
      onDataAvailable(ByteString.empty());
    }

    //todo consider malformed bodies of all sorts! premature onDone()? You bet!
    //todo consider no parts, or just one part, or even just one tiny part that is empty
    //todo max header limit - open JIRA
    //todo when using sublist, keep in mind that it will prevent garbage collection of parent list
    @Override
    public void onDataAvailable(ByteString data) {

      //todo check to see if things are done

      //We buffer forward a bit if we are out of data or if the data we have is the size of the boundary
      //This is to cover the case where a client asks for data but we have ONLY the boundary in buffer
      //which would then lead us to giving the client empty data when we call onPartDataAvailable().
      if (_byteList.size() <= _boundaryBytes.size()) {
        _rh.request(1);
        return;
      }

      //All operations will require us to buffer
      appendByteStringToList(data);

      if (_readState == ReadState.READING_PREAMBLE) {

        //todo improve this, this is n^2 - Consider using Google guava Bytes.indexof
        int tempLookup = Collections.indexOfSubList(_byteList, _boundaryBytes);
        if (tempLookup > -1) {
          final List<Byte> preambleBytes = _byteList.subList(0, tempLookup);
          _preamble = new String(ArrayUtils.toPrimitive((Byte[]) preambleBytes.toArray()));
          _byteList = _byteList.subList(tempLookup, _byteList.size());
          _readState = ReadState.PART_READING;
        } else {
          _rh.request(1);
          return;
        }
      }

      if (_readState == ReadState.PART_READING) {
        //todo improve this begins with
        final int boundaryIndex = Collections.indexOfSubList(_byteList, _boundaryBytes);

        if (boundaryIndex == 0) { //buffer begins with boundary

          //Close the current single part reader (except if this is the first boundary)
          if (_currentSinglePartMIMEReader != null) {
            _currentSinglePartMIMEReader._callback.onFinished();
            _currentSinglePartMIMEReader._isFinished = true;
            //we will now move on to notify the reader of the next part
          }

          //Now read until we have all the headers. Headers may or may not exist. According to the RFC:
          //If the headers do not exist, we will see two CRLFs one after another.
          //If atleast one header does exist, we will see the headers followed by two CRLFs
          //Essentially we are looking for the first occurrence of two CRLFs after we see the boundary.

          //We need to make sure we can look ahead a bit here first
          final int boundaryEnding = boundaryIndex + _boundaryBytes.size();
          if ((boundaryEnding + MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST.size()) > _byteList.size()) {
            _rh.request(1);
            return;
          }

          //Now determine the existence of headers
          final List<Byte> possibleHeaderArea = _byteList.subList(boundaryEnding, _byteList.size());
          final int headerEnding =
              Collections.indexOfSubList(possibleHeaderArea, MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST);
          if (headerEnding == -1) {
            _rh.request(1);
            return;
          }

          //Now we found the end. Let's make a window into the header area.
          final List<Byte> headerByteSubList = _byteList.subList(boundaryEnding, headerEnding);

          final Map<String, String> headers;
          if (headerByteSubList.equals(MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTE_LIST)) {
            //we have no headers
            headers = Collections.emptyMap();
          } else {
            headers = new HashMap<String, String>();
            //We have headers, lets read them in - we search using a sliding window
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
          //which means we can dump everything else beforehand
          _byteList = _byteList.subList(headerEnding, _byteList.size());

          //Notify the callback that we have a new part
          _currentSinglePartMIMEReader = new SinglePartMIMEReader(headers);
          _clientCallback.onNewPart(_currentSinglePartMIMEReader);
          return;
        } else {
          //buffer does not begin with boundary

          //we only proceed forward if the reader is ready - otherwise we won't have a current single part reader
          //to notify. In such a case we just return and move on (we already read into the buffer)
          //since the reader can then drive the flow of future data
          if (_readerReady) {
            if (boundaryIndex > -1) {
              //boundary is in buffer
              final List<Byte> useableBytes = _byteList.subList(0, boundaryIndex);
              _byteList = _byteList.subList(boundaryIndex, _byteList.size());
              _currentSinglePartMIMEReader._callback
                  .onPartDataAvailable(ByteString.copy(ArrayUtils.toPrimitive((Byte[]) useableBytes.toArray())));
              _readerReady = false;
              _rh.request(1); //this will call finish on this part and then create the subsequent part (if applicable)
            } else {
              //It doesn't exist here, so let's drain the buffer.
              //Note that we can't fully drain the buffer because the end of the buffer may include the beginning of the boundary
              //Therefore we grab the whole buffer but we leave the last boundaryBytes.size() number of bytes.
              //This is so that we are guaranteed that future appends to the _byteList will result in atleast one
              //byte available for further processing before the boundary is reached.
              //todo think about this one more time, then do done, then do abort
              final List<Byte> useableBytes = _byteList.subList(0, _byteList.size() - _boundaryBytes.size());
              _byteList = _byteList.subList(_byteList.size() - _boundaryBytes.size(), _byteList.size());
              _currentSinglePartMIMEReader._callback
                  .onPartDataAvailable(ByteString.copy(ArrayUtils.toPrimitive((Byte[]) useableBytes.toArray())));
              _readerReady = false;
            }
          }
        }
      }
    }

    @Override
    public void onDone() {
      //todo what happens if there was no data to begin with at all?
      //todo handle illicit or incomplete multipart mime requests
      //todo any multithreaded considerations?
    }

    @Override
    public void onError(Throwable e) {

    }

    private void appendByteStringToList(final ByteString byteString) {
      final byte[] byteStringArray = byteString.copyBytes();
      for (final byte b : byteStringArray) {
        _byteList.add(b);
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
    READING_PREAMBLE,
    PART_READING,
    ABORTING,
    READING_EPILOGUE,
    DONE,
    //not needed:
    READING_HEADER,
    IDLE,
    NOTIFY_PART,
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

  public class SinglePartMIMEReader implements MultiPartMIMEDataSource {

    private final Map<String, String> _headers;
    private SinglePartMIMEReaderCallback _callback = null;
    private final R2MultiPartMimeReader _r2MultiPartMimeReader;
    private boolean _isFinished = false;

    //Only MultiPartMIMEReader should ever create an instance
    private SinglePartMIMEReader(Map<String, String> headers) {
      _r2MultiPartMimeReader = MultiPartMIMEReader.this._reader;
      _headers = headers;
    }

    //This call commits and binds this callback to finishing this part. This can
    //only happen once per life of each SinglePartMIMEReader.
    //Meaning PartBindException will be thrown if there are attempts to mutate this callback
    public void registerReaderCallback(SinglePartMIMEReaderCallback callback)
        throws PartBindException {
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
    //todo - consider allowing multiple parts
    //todo - We would then have to serially provide all of them one after another
    public void requestPartData()
        throws IllegalArgumentException, PartNotInitializedException, StreamBusyException {

      if (_callback == null) {
        throw new PartNotInitializedException();
      }

      if (_r2MultiPartMimeReader._readState == ReadState.NOTIFY_PART) {
        //already busy fulfilling requests
        throw new StreamBusyException();
      }

      _r2MultiPartMimeReader._readerReady = true;
      //We can't request more data on behalf of the r2 reader, but we can refresh our current status and signal
      //the reader
      _r2MultiPartMimeReader.signalR2Reader();
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
    public void abandonPart()
        throws PartFinishedException, StreamBusyException {

    }

    @Override
    public void onInit(DataSourceHandle dataSourceHandle) {

    }

    @Override
    public void onWritePossible() {

    }

    @Override
    public void onAbort(Throwable e) {

    }

    @Override
    public Map<String, String> dataSourceHeaders() {
      return null;
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
  public void abandonAllParts()
      throws StreamBusyException, StreamFinishedException, ReaderNotInitializedException {

  }

  //Package Private reader callback registration
  //Used ONLY by a MultiPartMIMEWriter when they want to take over the rest of this stream.
  //This can ONLY be called if there is no part being actively read, meaning that
  //the current SinglePartMIMEReader has had no callback registered. Violation of this
  //will throw StreamBusyException.
  //This can be set even if no parts in the stream have actually been consumed, i.e
  //after the very first invocation of onNewPart() on the initial MultiPartMIMEReaderCallback.
  void registerReaderCallback(MultiPartMIMEReaderCallback clientCallback)
      throws StreamBusyException {
    _clientCallback = clientCallback;
    _entityStream.setReader(_reader);
  }
}
