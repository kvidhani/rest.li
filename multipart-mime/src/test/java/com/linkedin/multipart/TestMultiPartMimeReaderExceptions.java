package com.linkedin.multipart;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.multipart.reader.exceptions.IllegalMimeFormatException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.mail.BodyPart;
import javax.mail.Header;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.multipart.DataSources._bodyLessBody;
import static com.linkedin.multipart.DataSources._largeDataSource;
import static com.linkedin.multipart.DataSources._smallDataSource;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Created by kvidhani on 7/21/15.
 */
public class TestMultiPartMimeReaderExceptions {

  private static final Logger log = LoggerFactory.getLogger(TestMultiPartMimeReaderExceptions.class);

  //We will have only one thread in our executor service so that when we submit tasks to write
  //data from the writer we do it in order.
  private static ExecutorService threadPoolExecutor;

  MultiPartMIMEAbandonReaderCallbackImpl _currentMultiPartMIMEReaderCallback;

  @BeforeTest
  public void setup() {
    threadPoolExecutor = Executors.newFixedThreadPool(5);
  }

  @AfterTest
  public void shutDown() {
    threadPoolExecutor.shutdownNow();
  }


  ///////////////////////////////////////////////////////////////////////////////////////


  @DataProvider(name = "multipleNormalBodiesDataSource")
  public Object[][] multipleNormalBodiesDataSource() throws Exception
  {
    final List<MimeBodyPart> bodyPartList = new ArrayList<MimeBodyPart>();
    bodyPartList.add(_largeDataSource);
    bodyPartList.add(_smallDataSource);
    bodyPartList.add(_bodyLessBody);
    bodyPartList.add(_largeDataSource);
    bodyPartList.add(_smallDataSource);
    bodyPartList.add(_bodyLessBody);

    return new Object[][] {
        { 1, bodyPartList  },
        //{ R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList }
    };
  }


  ///////////////////////////////////////////////////////////////////////////////////////



  //todo use your own writer to do this
  /*
  @Test
  public void testEmptyEnvelope() throws Exception
  {
    //Javax mail does not support this, hence the reason for it only existing here where we test our custom reader.
    //final MultiPartMIMEWriter writer =
    //    new MultiPartMIMEWriter(Collections.<MultiPartMIMEDataPart>emptyList(), "some preamble", "some epilogue");
    //sendRequestAndAssert(Collections.<MultiPartMIMEDataPart>emptyList(), writer);
  }

  @Test
  public void missingContentTypeHeader()
  {

    final RestRequest multipartMimeRequest =
        new RestRequestBuilder(createHttpURI(PORT, SERVER_URI)).setMethod("POST").setEntity(ByteString.empty()).build();

    try
    {
      MultiPartMIMEReader.createMultiPartMIMEReader(multipartMimeRequest);
      Assert.fail();
    }
    catch (IllegalMimeFormatException illegalMimeFormatException)
    {
      Assert.assertEquals(illegalMimeFormatException.getMessage(), "No Content-Type header in this request");
    }
  }

  @Test
  public void invalidContentType()
  {
    final String contentTypeHeaderValue = "SomeErroneousContentType";

    final RestRequest multipartMimeRequest = new RestRequestBuilder(createHttpURI(PORT, SERVER_URI)).setMethod("POST")
        .addHeaderValue(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, contentTypeHeaderValue).setEntity(ByteString.empty())
        .build();

    try
    {
      MultiPartMIMEReader.createMultiPartMIMEReader(multipartMimeRequest);
      Assert.fail();
    }
    catch (IllegalMimeFormatException illegalMimeFormatException)
    {
      Assert.assertEquals(illegalMimeFormatException.getMessage(), "Not a valid multipart mime header");
    }
  }


  @Test
  public void payloadMissingBoundary() throws IllegalMimeFormatException
  {
    final String contentTypeHeaderValue = MultiPartMIMEUtils
        .buildMIMEContentTypeHeader("mixed", MultiPartMIMEUtils.generateBoundary(),
            Collections.<String, String>emptyMap());

    final RestRequest multipartMimeRequest = new RestRequestBuilder(createHttpURI(PORT, SERVER_URI)).setMethod("POST")
        .addHeaderValue(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, contentTypeHeaderValue).setEntity(ByteString.empty())
        .build();

    MultiPartMIMEReader multiPartMIMEReader = null;
    try
    {
      multiPartMIMEReader = MultiPartMIMEReader.createMultiPartMIMEReader(multipartMimeRequest);
      multiPartMIMEReader.parseAndReturnParts();
      Assert.fail();
    }
    catch (IllegalMimeFormatException illegalMimeFormatException)
    {
      Assert.assertEquals(illegalMimeFormatException.getMessage(), "Malformed multipart mime request. No boundary found!");
      Assert.assertEquals(multiPartMIMEReader.parseAndReturnParts(), Collections.<MultiPartMIMEDataPart>emptyList());
    }
  }

  */

  //todo when the reader in multipart mime gets called onError() by r2

  @Test(dataProvider = "multipleNormalBodiesDataSource")
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
    //TODO CHANGE THIS BACK TO TEN...maybe more?
    //Subtract 3 bytes since this end in CRLF which is 2 bytes
    final byte[] trimmedMimePayload = Arrays.copyOf(mimePayload, mimePayload.length - 3);

    final ByteString requestPayload = ByteString.copy(trimmedMimePayload);

    executeRequestWithDesiredException(requestPayload, chunkSize, multiPartMimeBody.getContentType(), "Malformed multipart mime request. Finishing boundary missing!");

    //todo should the last single part get a stream error

    //In this case we want all the parts to still make it over
    List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), multiPartMimeBody.getCount());

    //todo can this logic be moved into a resuable area
    //the last one should have gotten a stream error
    for (int i = 0; i<singlePartMIMEReaderCallbacks.size() -1;i++)
    {
      //Actual
      final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
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
    SinglePartMIMEAbandonReaderCallbackImpl singlePartMIMEAbandonReaderCallback =
        singlePartMIMEReaderCallbacks.get(singlePartMIMEReaderCallbacks.size() - 1);
    Assert.assertNull(singlePartMIMEAbandonReaderCallback._finishedData);
    Assert.assertTrue(singlePartMIMEAbandonReaderCallback._streamError instanceof IllegalMimeFormatException);
  }


  /*
  @Test
  public void prematureHeaderTermination() throws IllegalMimeFormatException
  {
    //Use Javax to create a multipart payload. Then we just modify the location of the consecutive CRLFs.
    final MimeMultipart multipartEnvelope = new MimeMultipart("mixed");
    final MimeBodyPart firstBody = new MimeBodyPart();
    try
    {
      firstBody.setContent("SomeBody", "text/plain");
      firstBody.setHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, "text/plain");
    }
    catch (Exception exception)
    {
      Assert.fail("Unable to create test data due to: " + exception);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    try
    {
      multipartEnvelope.addBodyPart(firstBody);
      multipartEnvelope.writeTo(byteArrayOutputStream);
    }
    catch (Exception exception)
    {
      Assert.fail("Unable to create test data due to: " + exception);
    }

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

    final RestRequest multipartMimeRequest = new RestRequestBuilder(createHttpURI(PORT, SERVER_URI)).setMethod("POST")
        .addHeaderValue(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, multipartEnvelope.getContentType())
        .setEntity(ByteString.copy(mimePayload)).build();

    MultiPartMIMEReader multiPartMIMEReader = null;
    try
    {
      multiPartMIMEReader = MultiPartMIMEReader.createMultiPartMIMEReader(multipartMimeRequest);
      multiPartMIMEReader.parseAndReturnParts();
      Assert.fail();
    }
    catch (IllegalMimeFormatException illegalMimeFormatException)
    {
      Assert.assertEquals(illegalMimeFormatException.getMessage(),
          "Malformed multipart mime request. Premature termination of headers within a part.");
      Assert.assertEquals(multiPartMIMEReader.parseAndReturnParts(), Collections.<MultiPartMIMEDataPart>emptyList());
    }
  }

  @Test
  public void incorrectHeaderStart() throws IllegalMimeFormatException
  {
    //Use Javax to create a multipart payload. Then we just find the CRLF before the first header and modify it.
    final MimeMultipart multipartEnvelope = new MimeMultipart("mixed");
    final MimeBodyPart firstBody = new MimeBodyPart();
    try
    {
      firstBody.setContent("SomeBody", "text/plain");
      firstBody.setHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, "text/plain");
    }
    catch (Exception exception)
    {
      Assert.fail("Unable to create test data due to: " + exception);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    try
    {
      multipartEnvelope.addBodyPart(firstBody);
      multipartEnvelope.writeTo(byteArrayOutputStream);
    }
    catch (Exception exception)
    {
      Assert.fail("Unable to create test data due to: " + exception);
    }

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

    final RestRequest multipartMimeRequest = new RestRequestBuilder(createHttpURI(PORT, SERVER_URI)).setMethod("POST")
        .addHeaderValue(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, multipartEnvelope.getContentType())
        .setEntity(ByteString.copy(mimePayload)).build();

    MultiPartMIMEReader multiPartMIMEReader = null;
    try
    {
      multiPartMIMEReader = MultiPartMIMEReader.createMultiPartMIMEReader(multipartMimeRequest);
      multiPartMIMEReader.parseAndReturnParts();
      Assert.fail();
    }
    catch (IllegalMimeFormatException illegalMimeFormatException)
    {
      Assert.assertEquals(illegalMimeFormatException.getMessage(), "Malformed multipart mime request. Headers are improperly constructed.");
      Assert.assertEquals(multiPartMIMEReader.parseAndReturnParts(), Collections.<MultiPartMIMEDataPart>emptyList());
    }
  }

  @Test
  public void incorrectHeaderFormat() throws IllegalMimeFormatException
  {
    //Use Javax to create a multipart payload. Then we just find an occurrence of the text/plain header and replace
    //the colon in that header to something else.
    final MimeMultipart multipartEnvelope = new MimeMultipart("mixed");
    final MimeBodyPart firstBody = new MimeBodyPart();
    try
    {
      firstBody.setContent("SomeBody", "text/plain");
      firstBody.setHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, "text/plain");
    }
    catch (Exception exception)
    {
      Assert.fail("Unable to create test data due to: " + exception);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    try
    {
      multipartEnvelope.addBodyPart(firstBody);
      multipartEnvelope.writeTo(byteArrayOutputStream);
    }
    catch (Exception exception)
    {
      Assert.fail("Unable to create test data due to: " + exception);
    }

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

    final RestRequest multipartMimeRequest = new RestRequestBuilder(createHttpURI(PORT, SERVER_URI)).setMethod("POST")
        .addHeaderValue(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, multipartEnvelope.getContentType())
        .setEntity(ByteString.copy(mimePayload)).build();

    MultiPartMIMEReader multiPartMIMEReader = null;
    try
    {
      multiPartMIMEReader = MultiPartMIMEReader.createMultiPartMIMEReader(multipartMimeRequest);
      multiPartMIMEReader.parseAndReturnParts();
      Assert.fail();
    }
    catch (IllegalMimeFormatException illegalMimeFormatException)
    {
      Assert.assertEquals(illegalMimeFormatException.getMessage(),
          "Malformed multipart mime request. Individual headers are improperly formatted.");
      Assert.assertEquals(multiPartMIMEReader.parseAndReturnParts(), Collections.<MultiPartMIMEDataPart>emptyList());
    }
  }


*/










  //todo - test with callback in construction



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
          threadPoolExecutor.submit(new Runnable() {
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
    Callback<Void> callback = expectFailureCallback(latch, throwable);



    //We simulate _client.streamRequest(request, callback);
    //MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
    //TestMultiPartMIMEReaderCallbackImpl _currentMultiPartMIMEReaderCallback =
    //    new TestMultiPartMIMEReaderCallbackImpl(callback);
    //reader.registerReaderCallback(_currentMultiPartMIMEReaderCallback);




    //      Callback<Integer> callback = expectSuccessCallback(latch, status, responseHeaders);
//        _client.streamRequest(request, callback);

    //todo assert the request has multipart content type

    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
    _currentMultiPartMIMEReaderCallback =
        new MultiPartMIMEAbandonReaderCallbackImpl(callback, reader);
    reader.registerReaderCallback(_currentMultiPartMIMEReaderCallback);




    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(throwable.get() instanceof IllegalMimeFormatException);
    Assert.assertEquals(throwable.get().getMessage(), desiredExceptionMessage);
    //assert the exception details





    //mock verifies

/*
    verify(streamRequest, times(1)).getEntityStream();
    verify(streamRequest, times(1)).getHeader(HEADER_CONTENT_TYPE);
    verify(entityStream, times(1)).setReader(isA(MultiPartMIMEReader.R2MultiPartMIMEReader.class));
    final int expectedRequests = (int)Math.ceil((double)requestPayload.length()/chunkSize);
    //One more expected request because we have to make the last call to get called onDone().
    verify(readHandle, times(expectedRequests + 1)).request(1);
    verifyNoMoreInteractions(streamRequest);
    verifyNoMoreInteractions(entityStream);
    verifyNoMoreInteractions(readHandle);
    */
  }



  static Callback<Void> expectFailureCallback(final CountDownLatch latch,
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



  private static class SinglePartMIMEAbandonReaderCallbackImpl implements SinglePartMIMEReaderCallback {

    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    static String _abandonValue;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData = null;
    Throwable _streamError = null;
    static int partCounter = 0;

    SinglePartMIMEAbandonReaderCallbackImpl(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
      _singlePartMIMEReader = singlePartMIMEReader;
      log.info("The headers for the current part " + partCounter + " are: ");
      log.info(singlePartMIMEReader.getHeaders().toString());
      _headers = singlePartMIMEReader.getHeaders();
    }

    @Override
    public void onPartDataAvailable(ByteString b) {
      log.info(
          "Just received " + b.length() + " byte(s) on the single part reader callback for part number " + partCounter);
      try {
        _byteArrayOutputStream.write(b.copyBytes());
      } catch (IOException ioException) {
        Assert.fail();
      }
      _singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished() {
      log.info("Part " + partCounter++ + " is done!");
      _finishedData = ByteString.copy(_byteArrayOutputStream.toByteArray());
    }

    //Delegate to the top level for now for these two
    @Override
    public void onAbandoned() {
      log.info("Part " + partCounter++ + " is finished being abandoned!");
    }

    @Override
    public void onStreamError(Throwable e) {
      _streamError = e;
    }
  }

  private static class MultiPartMIMEAbandonReaderCallbackImpl implements MultiPartMIMEReaderCallback {

    final Callback<Void> _r2callback;
    final MultiPartMIMEReader _reader;
    final List<SinglePartMIMEAbandonReaderCallbackImpl> _singlePartMIMEReaderCallbacks =
        new ArrayList<SinglePartMIMEAbandonReaderCallbackImpl>();

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {

      SinglePartMIMEAbandonReaderCallbackImpl singlePartMIMEReaderCallback =
          new SinglePartMIMEAbandonReaderCallbackImpl(singlePartMIMEReader);
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
    public void onStreamError(Throwable e) {
      _r2callback.onError(e);
    }

    MultiPartMIMEAbandonReaderCallbackImpl(final Callback<Void> r2callback,
        final MultiPartMIMEReader reader) {
      _r2callback = r2callback;
      _reader = reader;
    }
  }



}
