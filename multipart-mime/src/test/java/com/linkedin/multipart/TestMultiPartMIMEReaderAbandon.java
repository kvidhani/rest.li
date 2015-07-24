package com.linkedin.multipart;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.multipart.reader.exceptions.StreamFinishedException;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

import static com.linkedin.multipart.DataSources.*;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verifyNoMoreInteractions;


/**
 * Created by kvidhani on 7/20/15.
 */
public class TestMultiPartMIMEReaderAbandon {

  private static ExecutorService threadPoolExecutor;

  MultiPartMIMEAbandonReaderCallbackImpl _currentMultiPartMIMEReaderCallback;
  MimeMultipart _currentMimeMultipartBody;

  @BeforeTest
  public void setup() {
    threadPoolExecutor = Executors.newFixedThreadPool(5);
  }

  @AfterTest
  public void shutDown() {
    threadPoolExecutor.shutdownNow();
  }

  private static final Logger log = LoggerFactory.getLogger(TestMultiPartMIMEReaderAbandon.class);
    private static final String ABANDON_HEADER = "AbandonMe";

    //Header values for different server side behavior:
    //Single part abandons all individually but doesn't use a callback:
    private static final String SINGLE_ALL_NO_CALLBACK = "SINGLE_ALL_NO_CALLBACK";
    //Top level abandons all without ever registering a reader with the SinglePartMIMEReader:
    private static final String TOP_ALL = "TOP_ALL";
    //Single part abandons the first 6 (using registered callbacks) and then the top level abandons all of remaining:
    private static final String SINGLE_PARTIAL_TOP_REMAINING = "SINGLE_PARTIAL_TOP_REMAINING";
    //Single part alternates between consumption and abandoning the first 6 parts (using registered callbacks), then top
    //level abandons all of remaining. This means that parts 0, 2, 4 will be consumed and parts 1, 3, 5 will be abandoned.
    private static final String SINGLE_ALTERNATE_TOP_REMAINING = "SINGLE_ALTERNATE_TOP_REMAINING";
    //Single part abandons all individually (using registered callbacks):
    private static final String SINGLE_ALL = "SINGLE_ALL";
    //Single part alternates between consumption and abandoning all the way through (using registered callbacks):
    private static final String SINGLE_ALTERNATE = "SINGLE_ALTERNATE";


    ///////////////////////////////////////////////////////////////////////////////////////

    @DataProvider(name = "allTypesOfBodiesDataSource")
    public Object[][] allTypesOfBodiesDataSource()
            throws Exception {
        final List<MimeBodyPart> bodyPartList = new ArrayList<MimeBodyPart>();
        bodyPartList.add(_smallDataSource);
        bodyPartList.add(_largeDataSource);
        bodyPartList.add(_headerLessBody);
        bodyPartList.add(_bodyLessBody);
        bodyPartList.add(_bytesBody);
        bodyPartList.add(_purelyEmptyBody);

        bodyPartList.add(_purelyEmptyBody);
        bodyPartList.add(_bytesBody);
        bodyPartList.add(_bodyLessBody);
        bodyPartList.add(_headerLessBody);
        bodyPartList.add(_largeDataSource);
        bodyPartList.add(_smallDataSource);

        return new Object[][]{
                {1, bodyPartList},
                {R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList}
        };
    }

    ///////////////////////////////////////////////////////////////////////////////////////

    @Test(dataProvider = "allTypesOfBodiesDataSource")
    public void testSingleAllNoCallback(final int chunkSize, final List<MimeBodyPart> bodyPartList)
            throws Exception {
        //Execute the request and verify the correct header came back to ensure the server took the proper abandon actions.
        executeRequestWithAbandonStrategy(chunkSize, bodyPartList, SINGLE_ALL_NO_CALLBACK, "onFinished");

        //Single part abandons all individually but doesn't use a callback:
      List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
          _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
        Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 0);
    }

    @Test(dataProvider = "allTypesOfBodiesDataSource")
    public void testAbandonAll(final int chunkSize, final List<MimeBodyPart> bodyPartList)
            throws Exception {
        //Execute the request and verify the correct header came back to ensure the server took the proper abandon actions.
        executeRequestWithAbandonStrategy(chunkSize, bodyPartList, TOP_ALL, "onAbandoned");

        //Top level abandons all
        List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
                _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
        Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 0);
    }

    @Test(dataProvider = "allTypesOfBodiesDataSource")
    public void testSinglePartialTopRemaining(final int chunkSize, final List<MimeBodyPart> bodyPartList)
            throws Exception {
        //Execute the request, verify the correct header came back to ensure the server took the proper abandon actions
        //and return the payload so we can assert deeper.
       executeRequestWithAbandonStrategy(chunkSize, bodyPartList, SINGLE_PARTIAL_TOP_REMAINING, "onAbandoned");

        //Single part abandons the first 6 then the top level abandons all of remaining
      List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
          _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;

        Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 6);

        for (int i = 0; i < singlePartMIMEReaderCallbacks.size(); i++) {
            //Actual 
            final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
            //Expected 
            final BodyPart currentExpectedPart = _currentMimeMultipartBody.getBodyPart(i);

            //Construct expected headers and verify they match 
            final Map<String, String> expectedHeaders = new HashMap<String, String>();
            final Enumeration allHeaders = currentExpectedPart.getAllHeaders();

            while (allHeaders.hasMoreElements()) {
                final Header header = (Header) allHeaders.nextElement();
                expectedHeaders.put(header.getName(), header.getValue());
            }

            Assert.assertEquals(currentCallback._headers, expectedHeaders);
            //Verify that the bodies are empty
            Assert.assertNull(currentCallback._finishedData);
        }
    }

    @Test(dataProvider = "allTypesOfBodiesDataSource")
    public void testSingleAlternateTopRemaining(final int chunkSize, final List<MimeBodyPart> bodyPartList)
            throws Exception {
        //Execute the request, verify the correct header came back to ensure the server took the proper abandon actions
        //and return the payload so we can assert deeper.
        executeRequestWithAbandonStrategy(chunkSize, bodyPartList, SINGLE_ALTERNATE_TOP_REMAINING, "onAbandoned");

        //Single part alternates between consumption and abandoning the first 6 parts, then top level abandons all of remaining.
        //This means that parts 0, 2, 4 will be consumed and parts 1, 3, 5 will be abandoned.
      List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
          _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;

        Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 6);

        //First the consumed
        for (int i = 0; i < singlePartMIMEReaderCallbacks.size(); i=i+2) {
            //Actual 
            final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
            //Expected 
            final BodyPart currentExpectedPart = _currentMimeMultipartBody.getBodyPart(i);

            //Construct expected headers and verify they match 
            final Map<String, String> expectedHeaders = new HashMap<String, String>();
            final Enumeration allHeaders = currentExpectedPart.getAllHeaders();

            while (allHeaders.hasMoreElements()) {
                final Header header = (Header) allHeaders.nextElement();
                expectedHeaders.put(header.getName(), header.getValue());
            }

            Assert.assertEquals(currentCallback._headers, expectedHeaders);

            //Verify the body matches
            if(currentExpectedPart.getContent() instanceof byte[])
            {
                Assert.assertEquals(currentCallback._finishedData.copyBytes(), currentExpectedPart.getContent());
            } else {
                //Default is String
                Assert.assertEquals(new String(currentCallback._finishedData.copyBytes()), currentExpectedPart.getContent());
            }
        }

        //Then the abandoned
        for (int i = 1; i < singlePartMIMEReaderCallbacks.size(); i=i+2) {
            //Actual 
            final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
            //Expected 
            final BodyPart currentExpectedPart = _currentMimeMultipartBody.getBodyPart(i);

            //Construct expected headers and verify they match 
            final Map<String, String> expectedHeaders = new HashMap<String, String>();
            final Enumeration allHeaders = currentExpectedPart.getAllHeaders();

            while (allHeaders.hasMoreElements()) {
                final Header header = (Header) allHeaders.nextElement();
                expectedHeaders.put(header.getName(), header.getValue());
            }

            Assert.assertEquals(currentCallback._headers, expectedHeaders);
            //Verify that the bodies are empty
            Assert.assertNull(currentCallback._finishedData, null);
        }
    }

    @Test(dataProvider = "allTypesOfBodiesDataSource")
    public void testSingleAll(final int chunkSize, final List<MimeBodyPart> bodyPartList)
            throws Exception {
        //Execute the request, verify the correct header came back to ensure the server took the proper abandon actions
        //and return the payload so we can assert deeper.
        executeRequestWithAbandonStrategy(chunkSize, bodyPartList, SINGLE_ALL, "onFinished");

        //Single part abandons all, one by one
      List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
          _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;

        Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 12);

        //Verify everything was abandoned
        for (int i = 0; i < singlePartMIMEReaderCallbacks.size(); i++) {
            //Actual 
            final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
            //Expected 
            final BodyPart currentExpectedPart = _currentMimeMultipartBody.getBodyPart(i);

            //Construct expected headers and verify they match 
            final Map<String, String> expectedHeaders = new HashMap<String, String>();
            final Enumeration allHeaders = currentExpectedPart.getAllHeaders();

            while (allHeaders.hasMoreElements()) {
                final Header header = (Header) allHeaders.nextElement();
                expectedHeaders.put(header.getName(), header.getValue());
            }

            Assert.assertEquals(currentCallback._headers, expectedHeaders);
            //Verify that the bodies are empty
            Assert.assertNull(currentCallback._finishedData);
        }
    }


    @Test(dataProvider = "allTypesOfBodiesDataSource")
    public void testSingleAlternate(final int chunkSize, final List<MimeBodyPart> bodyPartList)
            throws Exception {
        //Execute the request, verify the correct header came back to ensure the server took the proper abandon actions
        //and return the payload so we can assert deeper.
        executeRequestWithAbandonStrategy(chunkSize, bodyPartList, SINGLE_ALTERNATE, "onFinished");

        //Single part alternates between consumption and abandoning for all 12 parts.
        //This means that parts 0, 2, 4, etc.. will be consumed and parts 1, 3, 5, etc... will be abandoned.
      List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
          _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;

        Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 12);

        //First the consumed
        for (int i = 0; i < singlePartMIMEReaderCallbacks.size(); i=i+2) {
            //Actual 
            final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
            //Expected 
            final BodyPart currentExpectedPart = _currentMimeMultipartBody.getBodyPart(i);

            //Construct expected headers and verify they match 
            final Map<String, String> expectedHeaders = new HashMap<String, String>();
            final Enumeration allHeaders = currentExpectedPart.getAllHeaders();

            while (allHeaders.hasMoreElements()) {
                final Header header = (Header) allHeaders.nextElement();
                expectedHeaders.put(header.getName(), header.getValue());
            }

            Assert.assertEquals(currentCallback._headers, expectedHeaders);

            //Verify the body matches
            if(currentExpectedPart.getContent() instanceof byte[])
            {
                Assert.assertEquals(currentCallback._finishedData.copyBytes(), currentExpectedPart.getContent());
            } else {
                //Default is String
                Assert.assertEquals(new String(currentCallback._finishedData.copyBytes()), currentExpectedPart.getContent());
            }
        }

        //Then the abandoned
        for (int i = 1; i < singlePartMIMEReaderCallbacks.size(); i=i+2) {
            //Actual 
            final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
            //Expected 
            final BodyPart currentExpectedPart = _currentMimeMultipartBody.getBodyPart(i);

            //Construct expected headers and verify they match 
            final Map<String, String> expectedHeaders = new HashMap<String, String>();
            final Enumeration allHeaders = currentExpectedPart.getAllHeaders();

            while (allHeaders.hasMoreElements()) {
                final Header header = (Header) allHeaders.nextElement();
                expectedHeaders.put(header.getName(), header.getValue());
            }

            Assert.assertEquals(currentCallback._headers, expectedHeaders);
            //Verify that the bodies are empty
            Assert.assertNull(currentCallback._finishedData);
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////

    private void executeRequestWithAbandonStrategy(final int chunkSize, final List<MimeBodyPart> bodyPartList,
                                                            final String abandonStrategy, final String serverHeaderPrefix)
            throws Exception {

        MimeMultipart multiPartMimeBody = new MimeMultipart();

        //Add your body parts
        for (final MimeBodyPart bodyPart : bodyPartList) {
            multiPartMimeBody.addBodyPart(bodyPart);
        }

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        multiPartMimeBody.writeTo(byteArrayOutputStream);
        final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());
    _currentMimeMultipartBody = multiPartMimeBody;

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

      final String contentTypeHeader = multiPartMimeBody.getContentType() + ";somecustomparameter=somecustomvalue"
          + ";anothercustomparameter=anothercustomvalue";
      when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);

      final AtomicInteger status = new AtomicInteger(-1);
      final CountDownLatch latch = new CountDownLatch(1);
      Callback<Integer> callback = expectSuccessCallback(latch, status);




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
                new MultiPartMIMEAbandonReaderCallbackImpl(callback, abandonStrategy, reader);
        reader.registerReaderCallback(_currentMultiPartMIMEReaderCallback);




        latch.await(60000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(status.get(), RestStatus.OK);
        Assert.assertEquals(_currentMultiPartMIMEReaderCallback._responseHeaders.get(ABANDON_HEADER), serverHeaderPrefix + abandonStrategy);


      try {
        reader.abandonAllParts();
        Assert.fail();
      } catch (StreamFinishedException streamFinishedException) {
        //pass
      }

      Assert.assertTrue(reader.haveAllPartsFinished());


      //mock verifies
      verify(streamRequest, times(1)).getEntityStream();
      verify(streamRequest, times(1)).getHeader(HEADER_CONTENT_TYPE);
      verify(entityStream, times(1)).setReader(isA(MultiPartMIMEReader.R2MultiPartMIMEReader.class));
      final int expectedRequests = (int)Math.ceil((double)requestPayload.length()/chunkSize);
      //One more expected request because we have to make the last call to get called onDone().
      verify(readHandle, times(expectedRequests + 1)).request(1);
      verifyNoMoreInteractions(streamRequest);
      verifyNoMoreInteractions(entityStream);
      verifyNoMoreInteractions(readHandle);
    }


  static Callback<Integer> expectSuccessCallback(final CountDownLatch latch,
      final AtomicInteger status)
  {
    return new Callback<Integer>()
    {
      @Override
      public void onError(Throwable e)
      {
        status.set(-1);
        latch.countDown();
      }

      @Override
      public void onSuccess(Integer result)
      {
        status.set(result);
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
            //MultiPartMIMEReader will end up calling onStreamError(e) on our top level callback
            //which will fail the test
        }
    }

    private static class MultiPartMIMEAbandonReaderCallbackImpl implements MultiPartMIMEReaderCallback {

        final Callback<Integer> _r2callback;
        final String _abandonValue;
        final MultiPartMIMEReader _reader;
      final Map<String, String> _responseHeaders = new HashMap<String, String>();
        final List<SinglePartMIMEAbandonReaderCallbackImpl> _singlePartMIMEReaderCallbacks =
                new ArrayList<SinglePartMIMEAbandonReaderCallbackImpl>();

        @Override
        public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {

            if (_abandonValue.equalsIgnoreCase(SINGLE_ALL_NO_CALLBACK)) {
                singlePartMIMEReader.abandonPart();
                return;
            }

            if (_abandonValue.equalsIgnoreCase(TOP_ALL)) {
                _reader.abandonAllParts();
                //todo what happens if they continue on from here? write a test for this
                return;
            }
            if (_abandonValue.equalsIgnoreCase(SINGLE_PARTIAL_TOP_REMAINING) && _singlePartMIMEReaderCallbacks.size() == 6) {
                _reader.abandonAllParts();
                return;
            }

            if (_abandonValue.equalsIgnoreCase(SINGLE_ALTERNATE_TOP_REMAINING)
                    && _singlePartMIMEReaderCallbacks.size() == 6) {
                _reader.abandonAllParts();
                return;
            }

            //Now we know we have to either consume or abandon individually using a registered callback, so we
            //register with the SinglePartReader and take appropriate action based on the abandon strategy:
            SinglePartMIMEAbandonReaderCallbackImpl singlePartMIMEReaderCallback =
                    new SinglePartMIMEAbandonReaderCallbackImpl(singlePartMIMEReader);
            singlePartMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
            _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);

            if (_abandonValue.equalsIgnoreCase(SINGLE_ALL) || _abandonValue.equalsIgnoreCase(SINGLE_PARTIAL_TOP_REMAINING)) {
                singlePartMIMEReader.abandonPart();
                return;
            }

            if (_abandonValue.equalsIgnoreCase(SINGLE_ALTERNATE) || _abandonValue
                    .equalsIgnoreCase(SINGLE_ALTERNATE_TOP_REMAINING)) {
                if (SinglePartMIMEAbandonReaderCallbackImpl.partCounter % 2 == 1) {
                    singlePartMIMEReader.abandonPart();
                } else {
                    singlePartMIMEReader.requestPartData();
                }
            }
        }

        @Override
        public void onFinished() {
            //Happens for SINGLE_ALL_NO_CALLBACK, SINGLE_ALL and SINGLE_ALTERNATE

          _responseHeaders.put(ABANDON_HEADER, "onFinished" + _abandonValue);
            _r2callback.onSuccess(200);
        }

        @Override
        public void onAbandoned() {
            //Happens for TOP_ALL, SINGLE_PARTIAL_TOP_REMAINING and SINGLE_ALTERNATE_TOP_REMAINING

          _responseHeaders.put(ABANDON_HEADER, "onAbandoned" + _abandonValue);
            _r2callback.onSuccess(200);
        }

        @Override
        public void onStreamError(Throwable e) {
            RestException restException = new RestException(RestStatus.responseForError(400, e));
            _r2callback.onError(restException);
        }

        MultiPartMIMEAbandonReaderCallbackImpl(final Callback<Integer> r2callback, final String abandonValue,
                                               final MultiPartMIMEReader reader) {
            _r2callback = r2callback;
            _abandonValue = abandonValue;
            _reader = reader;
            SinglePartMIMEAbandonReaderCallbackImpl._abandonValue = _abandonValue;
        }
    }


}
