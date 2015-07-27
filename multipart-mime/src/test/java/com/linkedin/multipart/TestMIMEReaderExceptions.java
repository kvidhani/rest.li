package com.linkedin.multipart;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.multipart.exceptions.IllegalMultiPartMIMEFormatException;
import com.linkedin.multipart.exceptions.PartFinishedException;
import com.linkedin.multipart.exceptions.ReaderFinishedException;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.mail.BodyPart;
import javax.mail.Header;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.*;

import static com.linkedin.multipart.DataSources.HEADER_CONTENT_TYPE;
import static com.linkedin.multipart.DataSources._bodyLessBody;
import static com.linkedin.multipart.DataSources._smallDataSource;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;


/**
 * Unit tests for testing various exceptions that could occur in the {@link com.linkedin.multipart.MultiPartMIMEReader}
 *
 * @author Karim Vidhani
 */
public class TestMIMEReaderExceptions extends AbstractMIMEUnitTest {
  MultiPartMIMEExceptionReaderCallbackImpl _currentMultiPartMIMEReaderCallback;

  @BeforeMethod
  public void setup() {
    _currentMultiPartMIMEReaderCallback = null;
  }

  @DataProvider(name = "multiplePartsDataSource")
  public Object[][] multiplePartsDataSource() throws Exception
  {
    final List<MimeBodyPart> bodyPartList = new ArrayList<MimeBodyPart>();
    bodyPartList.add(_smallDataSource);
    bodyPartList.add(_bodyLessBody);

    return new Object[][] {
        { 1, bodyPartList  },
        { R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList }
    };
  }

  //These tests all verify that we throw the correct exception in the face of RFC violating bodies:
  @Test
  public void missingContentTypeHeader()
  {
    StreamRequest streamRequest = null;
    try {

      streamRequest = mock(StreamRequest.class);
      when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(null);

      MultiPartMIMEReader.createAndAcquireStream(streamRequest);;
      Assert.fail();
    } catch (IllegalMultiPartMIMEFormatException illegalMimeFormatException) {
      Assert.assertEquals(illegalMimeFormatException.getMessage(),
          "Malformed multipart mime request. No Content-Type header in this request");

      verify(streamRequest, times(1)).getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER);
    }
  }

  @Test
  public void invalidContentType() throws Exception
  {
    StreamRequest streamRequest = null;
    try {

      streamRequest = mock(StreamRequest.class);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn("Some erroneous content type");

    MultiPartMIMEReader.createAndAcquireStream(streamRequest);;
      Assert.fail();
    } catch (IllegalMultiPartMIMEFormatException illegalMimeFormatException) {
      Assert.assertEquals(illegalMimeFormatException.getMessage(),
          "Malformed multipart mime request. Not a valid multipart mime header.");
      verify(streamRequest, times(1)).getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER);
    }
  }

  @Test(dataProvider = "chunkSizes")
  public void payloadMissingBoundary(final int chunkSize) throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();
    executeRequestWithDesiredException(ByteString.copy("This body has no boundary and is therefore not a valid multipart mime request".getBytes()),
        chunkSize, multiPartMimeBody.getContentType(), "Malformed multipart mime request. No boundary found!");

    //No single part readers should have been created.
    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 0);
  }

  @Test(dataProvider = "multiplePartsDataSource")
  public void payloadMissingFinalBoundary(final int chunkSize, final List<MimeBodyPart> bodyPartList)   throws
                                                                                                        Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList) {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);

    final byte[] mimePayload = byteArrayOutputStream.toByteArray();

    //To simulate the missing boundary, we have to trim 3 bytes off of the end. We need to snip the very last 2 bytes
    //because javax mail places a CRLF at the very end (which is not needed) and then another byte before that (which is a
    //hyphen) so that the final boundary never occurs.
    final byte[] trimmedMimePayload = Arrays.copyOf(mimePayload, mimePayload.length - 3);

    final ByteString requestPayload = ByteString.copy(trimmedMimePayload);
    executeRequestWithDesiredException(requestPayload, chunkSize, multiPartMimeBody.getContentType(), "Malformed multipart mime request. Finishing boundary missing!");

    List<SinglePartMIMEExceptionReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), multiPartMimeBody.getCount());

    //The last one should have gotten a stream error
    for (int i = 0; i<singlePartMIMEReaderCallbacks.size() -1;i++)
    {
      //Actual
      final SinglePartMIMEExceptionReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
      //Expected
      final BodyPart currentExpectedPart = multiPartMimeBody.getBodyPart(i);

      //Construct expected headers and verify they match
      final Map<String, String> expectedHeaders = new HashMap<String, String>();
      final Enumeration allHeaders = currentExpectedPart.getAllHeaders();
      while (allHeaders.hasMoreElements())
      {
        final Header header = (Header) allHeaders.nextElement();
        expectedHeaders.put(header.getName(), header.getValue());
      }
      Assert.assertEquals(currentCallback._headers, expectedHeaders);

      //Verify the body matches
      Assert.assertNotNull(currentCallback._finishedData);
      if(currentExpectedPart.getContent() instanceof byte[])
      {
        Assert.assertEquals(currentCallback._finishedData.copyBytes(), currentExpectedPart.getContent());
      } else {
        //Default is String
        Assert.assertEquals(new String(currentCallback._finishedData.copyBytes()), currentExpectedPart.getContent());
      }
    }

    SinglePartMIMEExceptionReaderCallbackImpl singlePartMIMEExceptionReaderCallback =
        singlePartMIMEReaderCallbacks.get(singlePartMIMEReaderCallbacks.size() - 1);
    Assert.assertNull(singlePartMIMEExceptionReaderCallback._finishedData);
    Assert.assertTrue(singlePartMIMEExceptionReaderCallback._streamError instanceof IllegalMultiPartMIMEFormatException);

    try {
      singlePartMIMEExceptionReaderCallback._singlePartMIMEReader.requestPartData();
      Assert.fail();
    } catch (PartFinishedException partFinishedException) {
      //pass
    }
  }

  @Test(dataProvider = "multiplePartsDataSource")
  public void boundaryPrematurelyTerminatedNoSubsequentCRLFs(final int chunkSize, final List<MimeBodyPart> bodyPartList)   throws
          Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList) {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);

    final byte[] mimePayload = byteArrayOutputStream.toByteArray();
    //At this point the mimePayload's ending looks something like the following. Consider that
    //--1234 is the boundary:
    //<ending of some part data>--1234--/r/n
    //What we want to test this particular code path is:
    //<ending of some part data>--1234678
    //So we trim off an element in the array at the end which results in:
    //<ending of some part data>--1234--/r
    //And then we modify the last three bytes to end up with:
    //<ending of some part data>--1234678

    final byte[] trimmedMimePayload = Arrays.copyOf(mimePayload, mimePayload.length - 1);
    trimmedMimePayload[trimmedMimePayload.length-1] = 8;
    trimmedMimePayload[trimmedMimePayload.length-2] = 7;
    trimmedMimePayload[trimmedMimePayload.length-3] = 6;

    final ByteString requestPayload = ByteString.copy(trimmedMimePayload);
    executeRequestWithDesiredException(requestPayload, chunkSize, multiPartMimeBody.getContentType(), "Malformed multipart mime request. Premature termination of multipart mime body due to a boundary without a subsequent consecutive CRLF.");

    //In this case we want all the parts to still make it over
    List<SinglePartMIMEExceptionReaderCallbackImpl> singlePartMIMEReaderCallbacks =
            _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), multiPartMimeBody.getCount());

    //Everything should have made it over
    for (int i = 0; i<singlePartMIMEReaderCallbacks.size();i++)
    {
      //Actual
      final SinglePartMIMEExceptionReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
      //Expected
      final BodyPart currentExpectedPart = multiPartMimeBody.getBodyPart(i);

      //Construct expected headers and verify they match
      final Map<String, String> expectedHeaders = new HashMap<String, String>();
      final Enumeration allHeaders = currentExpectedPart.getAllHeaders();
      while (allHeaders.hasMoreElements())
      {
        final Header header = (Header) allHeaders.nextElement();
        expectedHeaders.put(header.getName(), header.getValue());
      }
      Assert.assertEquals(currentCallback._headers, expectedHeaders);

      //Verify the body matches
      Assert.assertNotNull(currentCallback._finishedData);
      if(currentExpectedPart.getContent() instanceof byte[])
      {
        Assert.assertEquals(currentCallback._finishedData.copyBytes(), currentExpectedPart.getContent());
      } else {
        //Default is String
        Assert.assertEquals(new String(currentCallback._finishedData.copyBytes()), currentExpectedPart.getContent());
      }
    }
  }

  @Test(dataProvider = "multiplePartsDataSource")
  public void prematureHeaderTermination(final int chunkSize, final List<MimeBodyPart> bodyPartList)   throws
                                                                                                        Exception

  {
    //Use Javax to create a multipart payload. Then we just modify the location of the consecutive CRLFs.
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList) {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);

    final byte[] mimePayload = byteArrayOutputStream.toByteArray();

    //Find where the consecutive CRLFs are after the occurrences of the headers and modify it
    for (int i = 0; i < mimePayload.length - 4; i++)
    {
      final byte[] currentWindow = Arrays.copyOfRange(mimePayload, i, i + 4);
      if (Arrays.equals(currentWindow, MultiPartMIMEUtils.CONSECUTIVE_CRLFS_BYTES))
      {
        mimePayload[i] = 15;
      }
    }

    final ByteString requestPayload = ByteString.copy(mimePayload);
    executeRequestWithDesiredException(requestPayload, chunkSize, multiPartMimeBody.getContentType(), "Malformed multipart mime request. Premature termination of headers within a part.");

    //No single part readers should have been created.
    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 0);
  }


  @Test(dataProvider = "multiplePartsDataSource")
  public void incorrectHeaderStart(final int chunkSize, final List<MimeBodyPart> bodyPartList)   throws
                                                                                                       Exception

  {
    //Use Javax to create a multipart payload. Then we just modify the location of the consecutive CRLFs.
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList) {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);

    final byte[] mimePayload = byteArrayOutputStream.toByteArray();

    //Find where the first CRLF is. Technically there should be a leading CRLF for the first boundary
    //but Javax mail doesn't do this.
    for (int i = 0; i < mimePayload.length - 2; i++)
    {
      final byte[] currentWindow = Arrays.copyOfRange(mimePayload, i, i + 2);
      if (Arrays.equals(currentWindow, MultiPartMIMEUtils.CRLF_BYTES))
      {
        mimePayload[i] = 15;
        break;
      }
    }

    final ByteString requestPayload = ByteString.copy(mimePayload);
    executeRequestWithDesiredException(requestPayload, chunkSize, multiPartMimeBody.getContentType(), "Malformed multipart mime request. Headers are improperly constructed.");

    //No single part readers should have been created.
    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 0);
  }

  @Test(dataProvider = "multiplePartsDataSource")
  public void incorrectHeaderFormat(final int chunkSize, final List<MimeBodyPart> bodyPartList)   throws
                                                                                                 Exception
  {
    //Use Javax to create a multipart payload. Then we just modify the location of the consecutive CRLFs.
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList) {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);

    final byte[] mimePayload = byteArrayOutputStream.toByteArray();


    final byte[] contentTypeColonBytes = (MultiPartMIMEUtils.CONTENT_TYPE_HEADER + ":").getBytes();
    for (int i = 0; i < mimePayload.length - contentTypeColonBytes.length; i++)
    {
      final byte[] currentWindow = Arrays.copyOfRange(mimePayload, i, i + contentTypeColonBytes.length);
      if (Arrays.equals(currentWindow, contentTypeColonBytes))
      {
        mimePayload[i + currentWindow.length - 1] = 15;
        break;
      }
    }

    final ByteString requestPayload = ByteString.copy(mimePayload);
    executeRequestWithDesiredException(requestPayload, chunkSize, multiPartMimeBody.getContentType(), "Malformed multipart mime request. Individual headers are improperly formatted.");

    //No single part readers should have been created.
    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 0);
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  //These tests will verify that we clean up properly when R2 invokes us with onError()

  //This can happen when we are already in an erroneous state. We want to make sure we don't invoke the client multiple times.
  //We want to create a state where we have already invoked the client on their onStreamError() callbacks once.
  @Test(dataProvider = "multiplePartsDataSource")
  public void alreadyErrorPreventDoubleInvocation(final int chunkSize, final List<MimeBodyPart> bodyPartList)   throws
                                                                                                        Exception
  {

    payloadMissingFinalBoundary(chunkSize, bodyPartList);

    //The asserts in the callback will make sure that we don't call the callbacks multiple times.
    //Also we have already verified that _rh.cancel() only occured once.
    _currentMultiPartMIMEReaderCallback._reader.getR2MultiPartMIMEReader().onError(new NullPointerException());

  }

  ///////////////////////////////////////////////////////////////////////////////////////
  private void executeRequestWithDesiredException(final ByteString requestPayload, final int chunkSize, final String contentTypeHeader,
      final String desiredExceptionMessage) throws Exception {

    final EntityStream entityStream = mock(EntityStream.class);
    final ReadHandle readHandle = mock(ReadHandle.class);

    //We have to use the AtomicReference holder technique to modify the current remaining buffer since the inner class
    //in doAnswer() can only access final variables.
    final AtomicReference<MultiPartMIMEReader.R2MultiPartMIMEReader> r2Reader =
        new AtomicReference<MultiPartMIMEReader.R2MultiPartMIMEReader>();

    //This takes the place of VariableByteStringWriter if we were to use R2 directly.
    final VariableByteStringViewer variableByteStringViewer = new VariableByteStringViewer(requestPayload, chunkSize);

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        final MultiPartMIMEReader.R2MultiPartMIMEReader reader = r2Reader.get();
        Object[] args = invocation.getArguments();

        //will always be 1 since MultiPartMIMEReader only does _rh.request(1)
        final int chunksRequested = (Integer)args[0];

        for (int i = 0;i<chunksRequested; i++) {
          //Our tests will run into a stack overflow unless we use a thread pool here to fire off the callbacks.
          //Especially in cases where the chunk size is 1. When the chunk size is one, the MultiPartMIMEReader
          //ends up doing many _rh.request(1) since each write is only 1 byte.
          //R2 uses a different technique to avoid stack overflows here which is unnecessary to emulate.
          _scheduledExecutorService.submit(new Runnable() {
            @Override
            public void run() {

              ByteString clientData = variableByteStringViewer.onWritePossible();
              if (clientData.equals(ByteString.empty())) {
                reader.onDone();
              }
              else {
                reader.onDataAvailable(clientData);
              }
            }
          });
        }
        return null;
      }
    }).when(readHandle).request(isA(Integer.class));

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final MultiPartMIMEReader.R2MultiPartMIMEReader reader = (MultiPartMIMEReader.R2MultiPartMIMEReader) args[0];
        r2Reader.set(reader);
        //R2 calls init immediately upon setting the reader
        reader.onInit(readHandle);
        return null;
      }
    }).when(entityStream).setReader(isA(MultiPartMIMEReader.R2MultiPartMIMEReader.class));

    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);

    final AtomicReference<Throwable> throwable = new AtomicReference<Throwable>();
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<Void> callback = generateFailureCallback(latch, throwable);

    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
    _currentMultiPartMIMEReaderCallback =
        new MultiPartMIMEExceptionReaderCallbackImpl(callback, reader);
    reader.registerReaderCallback(_currentMultiPartMIMEReaderCallback);

    latch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    //Verify the correct exception was sent to the reader callback. The test itself will then verify
    //if the correct error (if applicable) was sent to the single part reader callback.
    Assert.assertTrue(throwable.get() instanceof IllegalMultiPartMIMEFormatException);
    Assert.assertEquals(throwable.get().getMessage(), desiredExceptionMessage);

    //Verify these are unusable.
    try {
      reader.abandonAllParts();
      Assert.fail();
    } catch (ReaderFinishedException readerFinishedException) {
      //pass
    }

    //Unnecessary to verify how many times requestData on the read handle was called.
    verify(readHandle, atLeastOnce()).request(isA(Integer.class));
    verify(readHandle, times(1)).cancel();
    verify(streamRequest, times(1)).getEntityStream();
    verify(streamRequest, times(1)).getHeader(HEADER_CONTENT_TYPE);
    verify(entityStream, times(1)).setReader(isA(MultiPartMIMEReader.R2MultiPartMIMEReader.class));
    verifyNoMoreInteractions(streamRequest);
    verifyNoMoreInteractions(entityStream);
  }

  private Callback<Void> generateFailureCallback(final CountDownLatch latch,
      final AtomicReference<Throwable> throwable)
  {

    return new Callback<Void>()
    {
      @Override
      public void onError(Throwable e)
      {
        throwable.set(e);
        latch.countDown();
      }

      @Override
      public void onSuccess(Void result)
      {
        latch.countDown();
      }
    };
  }

  private static class SinglePartMIMEExceptionReaderCallbackImpl implements SinglePartMIMEReaderCallback {
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData = null;
    Throwable _streamError = null;

    SinglePartMIMEExceptionReaderCallbackImpl(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
      _singlePartMIMEReader = singlePartMIMEReader;
      _headers = singlePartMIMEReader.getHeaders();
    }

    @Override
    public void onPartDataAvailable(ByteString partData) {
      try {
        _byteArrayOutputStream.write(partData.copyBytes());
      } catch (IOException ioException) {
        Assert.fail();
      }
      _singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished() {
      _finishedData = ByteString.copy(_byteArrayOutputStream.toByteArray());
    }

    @Override
    public void onAbandoned() {
    }

    @Override
    public void onStreamError(Throwable throwable) {
      //Should only happen once.
      if (_streamError !=null) {
        Assert.fail();
      }
      _streamError = throwable;
    }
  }

  private static class MultiPartMIMEExceptionReaderCallbackImpl implements MultiPartMIMEReaderCallback {

    final Callback<Void> _r2callback;
    final MultiPartMIMEReader _reader;
    final List<SinglePartMIMEExceptionReaderCallbackImpl> _singlePartMIMEReaderCallbacks =
        new ArrayList<SinglePartMIMEExceptionReaderCallbackImpl>();
    Throwable _streamError = null;

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {

      SinglePartMIMEExceptionReaderCallbackImpl singlePartMIMEReaderCallback =
          new SinglePartMIMEExceptionReaderCallbackImpl(singlePartMIMEReader);
      singlePartMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
      _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);

     singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished() {
      _r2callback.onSuccess(null);
    }

    @Override
    public void onAbandoned() {
      _r2callback.onSuccess(null);
    }

    @Override
    public void onStreamError(Throwable throwable) {
      //We should only ever be invoked once.
      if (_streamError != null) {
        Assert.fail();
      }
      _streamError = throwable;
      _r2callback.onError(throwable);
    }

    MultiPartMIMEExceptionReaderCallbackImpl(final Callback<Void> r2callback,
                                             final MultiPartMIMEReader reader) {
      _r2callback = r2callback;
      _reader = reader;
    }
  }
}