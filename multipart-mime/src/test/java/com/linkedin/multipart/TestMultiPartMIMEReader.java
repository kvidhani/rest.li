package com.linkedin.multipart;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
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
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.multipart.DataSources.*;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;


/**
 * Created by kvidhani on 7/19/15.
 */
public class TestMultiPartMIMEReader {

  private static int TEST_TIMEOUT = 60000;

    //We will have only one thread in our executor service so that when we submit tasks to write
    //data from the writer we do it in order.
    private static ExecutorService threadPoolExecutor;
    private MultiPartMIMEReader _reader;

    @BeforeTest
    public void setup() {
        threadPoolExecutor = Executors.newFixedThreadPool(5);
    }

    @AfterTest
    public void shutDown() {
        threadPoolExecutor.shutdownNow();
    }

    //Javax mail always includes a final, trailing CRLF after the final boundary. Meaning something like
    //--myFinalBoundary--/r/n
    //
    //This trailing CRLF is not considered part of the final boundary and is, presumably, some sort of default
    //epilogue. We want to remove this, otherwise all of our data sources in all of our tests will always have some sort
    //of epilogue at the end and we won't have any tests where the data sources end with JUST the final boundary.
    private ByteString trimTrailingCRLF(final ByteString javaxMailPayload) {
       //Assert the trailing CRLF does
        final byte[] javaxMailPayloadBytes = javaxMailPayload.copyBytes();
        //Verify, in case the version of javax mail is changed, that the last two bytes are still CRLF (13 and 10).
        Assert.assertEquals(javaxMailPayloadBytes[javaxMailPayloadBytes.length-2], 13);
        Assert.assertEquals(javaxMailPayloadBytes[javaxMailPayloadBytes.length-1], 10);
        return javaxMailPayload.copySlice(0, javaxMailPayload.length()-2);
    }

    ///////////////////////////////////////////////////////////////////////////////////////

    @DataProvider(name = "eachSingleBodyDataSource")
    public Object[][] eachSingleBodyDataSource() throws Exception
    {
        return new Object[][] {
                { 1, _smallDataSource  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _smallDataSource },
                { 1, _largeDataSource  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _largeDataSource },
                { 1, _headerLessBody  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _headerLessBody },
                { 1, _bodyLessBody  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _bodyLessBody },
                { 1, _bytesBody  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _bytesBody },
                { 1, _purelyEmptyBody  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _purelyEmptyBody },

        };
    }

    @Test(dataProvider = "eachSingleBodyDataSource")
    public void testEachSingleBodyDataSource(final int chunkSize, final MimeBodyPart bodyPart) throws Exception
    {
        MimeMultipart multiPartMimeBody = new MimeMultipart();

        //Add your body parts
        multiPartMimeBody.addBodyPart(bodyPart);
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        multiPartMimeBody.writeTo(byteArrayOutputStream);
        final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());
        //final VariableByteStringWriter variableByteStringWriter = new VariableByteStringWriter(requestPayload, chunkSize);
        executeRequestAndAssert(trimTrailingCRLF(requestPayload), chunkSize, multiPartMimeBody);
    }

    @Test(dataProvider = "eachSingleBodyDataSource")
    public void testEachSingleBodyDataSourceMultipleTimes(final int chunkSize, final MimeBodyPart bodyPart) throws Exception
    {
        MimeMultipart multiPartMimeBody = new MimeMultipart();

        //Add your body parts
        multiPartMimeBody.addBodyPart(bodyPart);
        multiPartMimeBody.addBodyPart(bodyPart);
        multiPartMimeBody.addBodyPart(bodyPart);
        multiPartMimeBody.addBodyPart(bodyPart);
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        multiPartMimeBody.writeTo(byteArrayOutputStream);
        final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());
        executeRequestAndAssert(trimTrailingCRLF(requestPayload), chunkSize, multiPartMimeBody);
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
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList }
        };
    }

    @Test(dataProvider = "multipleNormalBodiesDataSource")
    public void testMultipleNormalBodiesDataSource(final int chunkSize, final List<MimeBodyPart> bodyPartList) throws Exception
    {
        MimeMultipart multiPartMimeBody = new MimeMultipart();

        //Add your body parts
        for (final MimeBodyPart bodyPart : bodyPartList) {
            multiPartMimeBody.addBodyPart(bodyPart);
        }

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        multiPartMimeBody.writeTo(byteArrayOutputStream);
        final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());
        executeRequestAndAssert(trimTrailingCRLF(requestPayload), chunkSize, multiPartMimeBody);
    }

    ///////////////////////////////////////////////////////////////////////////////////////

    @DataProvider(name = "multipleAbnormalBodies")
    public Object[][] multipleAbnormalBodies() throws Exception
    {
        final List<MimeBodyPart> bodyPartList = new ArrayList<MimeBodyPart>();
        bodyPartList.add(_headerLessBody);
        bodyPartList.add(_bodyLessBody);
        bodyPartList.add(_purelyEmptyBody);

        return new Object[][] {
                { 1, bodyPartList  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList }
        };
    }

    @Test(dataProvider = "multipleAbnormalBodies")
    public void testMultipleAbnormalBodies(final int chunkSize, final List<MimeBodyPart> bodyPartList) throws Exception
    {
        MimeMultipart multiPartMimeBody = new MimeMultipart();

        //Add your body parts
        for (final MimeBodyPart bodyPart : bodyPartList) {
            multiPartMimeBody.addBodyPart(bodyPart);
        }

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        multiPartMimeBody.writeTo(byteArrayOutputStream);
        final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());
        executeRequestAndAssert(trimTrailingCRLF(requestPayload), chunkSize, multiPartMimeBody);
    }


    ///////////////////////////////////////////////////////////////////////////////////////

    @DataProvider(name = "allTypesOfBodiesDataSource")
    public Object[][] allTypesOfBodiesDataSource() throws Exception
    {
        final List<MimeBodyPart> bodyPartList = new ArrayList<MimeBodyPart>();
        bodyPartList.add(_smallDataSource);
        bodyPartList.add(_largeDataSource);
        bodyPartList.add(_headerLessBody);
        bodyPartList.add(_bodyLessBody);
        bodyPartList.add(_bytesBody);
        bodyPartList.add(_purelyEmptyBody);

        return new Object[][] {
                { 1, bodyPartList  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList }
        };
    }

    @Test(dataProvider = "allTypesOfBodiesDataSource")
    public void testAllTypesOfBodiesDataSource(final int chunkSize, final List<MimeBodyPart> bodyPartList) throws Exception
    {
        MimeMultipart multiPartMimeBody = new MimeMultipart();

        //Add your body parts
        for (final MimeBodyPart bodyPart : bodyPartList) {
            multiPartMimeBody.addBodyPart(bodyPart);
        }

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        multiPartMimeBody.writeTo(byteArrayOutputStream);
        final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());
        executeRequestAndAssert(trimTrailingCRLF(requestPayload), chunkSize, multiPartMimeBody);
    }


    ///////////////////////////////////////////////////////////////////////////////////////

    @DataProvider(name = "preambleEpilogueDataSource")
    public Object[][] preambleEpilogueDataSource() throws Exception
    {
        final List<MimeBodyPart> bodyPartList = new ArrayList<MimeBodyPart>();
        bodyPartList.add(_smallDataSource);
        bodyPartList.add(_largeDataSource);
        bodyPartList.add(_headerLessBody);
        bodyPartList.add(_bodyLessBody);
        bodyPartList.add(_bytesBody);
        bodyPartList.add(_purelyEmptyBody);

        return new Object[][] {
                { 1, bodyPartList, null, null  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList, null, null },
                { 1, bodyPartList, "Some preamble", "Some epilogue"  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList, "Some preamble", "Some epilogue"  },
                { 1, bodyPartList, "Some preamble", null  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList, "Some preamble", null },
                { 1, bodyPartList, null,  "Some epilogue"  },
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList, null,  "Some epilogue" }
        };
    }

    //Just test the preamble and epilogue here
    @Test(dataProvider = "preambleEpilogueDataSource")
    public void testPreambleAndEpilogue(final int chunkSize, final List<MimeBodyPart> bodyPartList, final String preamble, final String epilogue) throws Exception
    {
        MimeMultipart multiPartMimeBody = new MimeMultipart();

        //Add your body parts
        for (final MimeBodyPart bodyPart : bodyPartList) {
            multiPartMimeBody.addBodyPart(bodyPart);
        }

        if (preamble != null) {
            multiPartMimeBody.setPreamble(preamble);
        }

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        multiPartMimeBody.writeTo(byteArrayOutputStream);

        final ByteString requestPayload;
        if (epilogue != null) {
            //Javax mail does not support epilogue so we add it ourselves (other then the CRLF following the final
            //boundary).
            byteArrayOutputStream.write(epilogue.getBytes());
            requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());
        } else {
            //Our test desired no epilogue.
            //Remove the CRLF introduced by javax mail at the end. We won't want a fake epilogue.
            requestPayload = trimTrailingCRLF(ByteString.copy(byteArrayOutputStream.toByteArray()));
        }

        executeRequestAndAssert(requestPayload, chunkSize, multiPartMimeBody);
    }


    ///////////////////////////////////////////////////////////////////////////////////////


    //Special test to make sure we don't stack overflow.
  //Have tons of small parts that are all read in at once due to the huge chunk size.
  @Test
  public void testStackOverflow() throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();
    TEST_TIMEOUT = 600000;

    //Add many tiny bodies. Since everything comes into memory on the first chunk, we will interact exclusively with the
    //client and not R2. We want to make sure that us calling them, and them calling us back, and us calling them over and over
    //does not lead to a stack overflow.
    for (int i = 0; i< 5000; i++) {
      multiPartMimeBody.addBodyPart(_tinyDataSource);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());
    executeRequestAndAssert(trimTrailingCRLF(requestPayload), Integer.MAX_VALUE, multiPartMimeBody);
  }

///////////////////////////////////////////////////////////////////////////////////////


  //This test will verify, that once we are successfully finished, that if R2 gives us onError() we don't let the client know.
  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void alreadyFinishedPreventErrorClient(final int chunkSize, final List<MimeBodyPart> bodyPartList)   throws
                                                                                                                Exception
  {
    testAllTypesOfBodiesDataSource(chunkSize, bodyPartList);

    //The asserts in the callback will make sure that we don't call onStreamError() on the callbacks.
    //Also we have already verified that _rh.cancel() did not occur.
    _reader.getR2MultiPartMIMEReader().onError(new NullPointerException());
  }





  ///////////////////////////////////////////////////////////////////////////////////////
    private void executeRequestAndAssert(final ByteString payload, final int chunkSize, final MimeMultipart mimeMultipart) throws Exception {

        //final EntityStream entityStream = EntityStreams.newEntityStream(dataSourceWriter);
        //final EntityStreams entityStreamClass = mock(EntityStreams.class);
        final EntityStream entityStream = mock(EntityStream.class);

        final ReadHandle readHandle = mock(ReadHandle.class);

        //We have to use the AtomicReference holder technique to modify the current remaining buffer since the inner class
        //in doAnswer() can only access final variables.
        final AtomicReference<MultiPartMIMEReader.R2MultiPartMIMEReader> r2Reader =
                new AtomicReference<MultiPartMIMEReader.R2MultiPartMIMEReader>();

        //This takes the place of VariableByteStringWriter if we were to use R2 directly.
        final VariableByteStringViewer variableByteStringViewer = new VariableByteStringViewer(payload, chunkSize);

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
        final String contentTypeHeader = mimeMultipart.getContentType() + ";somecustomparameter=somecustomvalue"
                + ";anothercustomparameter=anothercustomvalue";
        when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);

        final AtomicInteger status = new AtomicInteger(-1);
        final CountDownLatch latch = new CountDownLatch(1);
        Callback<Integer> callback = expectSuccessCallback(latch, status);

        //We simulate _client.streamRequest(request, callback);
        _reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
        TestMultiPartMIMEReaderCallbackImpl _testMultiPartMIMEReaderCallback =
                new TestMultiPartMIMEReaderCallbackImpl(callback);
        _reader.registerReaderCallback(_testMultiPartMIMEReaderCallback);

        latch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(status.get(), RestStatus.OK);

        List<TestSinglePartMIMEReaderCallbackImpl> singlePartMIMEReaderCallbacks =
                _testMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
        Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), mimeMultipart.getCount());
        for (int i = 0; i<singlePartMIMEReaderCallbacks.size();i++)
        {
            //Actual
            final TestSinglePartMIMEReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
            //Expected
            final BodyPart currentExpectedPart = mimeMultipart.getBodyPart(i);

            //Construct expected headers and verify they match
            final Map<String, String> expectedHeaders = new HashMap<String, String>();
            final Enumeration allHeaders = currentExpectedPart.getAllHeaders();
            while (allHeaders.hasMoreElements())
            {
                final Header header = (Header) allHeaders.nextElement();
                expectedHeaders.put(header.getName(), header.getValue());
            }
            Assert.assertEquals(currentCallback._headers, expectedHeaders);

          Assert.assertNotNull(currentCallback._finishedData);
            //Verify the body matches
            if(currentExpectedPart.getContent() instanceof byte[])
            {
                Assert.assertEquals(currentCallback._finishedData.copyBytes(), currentExpectedPart.getContent());
            } else {
                //Default is String
                Assert.assertEquals(new String(currentCallback._finishedData.copyBytes()), currentExpectedPart.getContent());
            }
        }

      //Mock verifies
      verify(streamRequest, times(1)).getEntityStream();
      verify(streamRequest, times(1)).getHeader(HEADER_CONTENT_TYPE);
      verify(entityStream, times(1)).setReader(isA(MultiPartMIMEReader.R2MultiPartMIMEReader.class));
      final int expectedRequests = (int)Math.ceil((double)payload.length()/chunkSize);
      //One more expected request because we have to make the last call to get called onDone().
      verify(readHandle, times(expectedRequests + 1)).request(1);
      verifyNoMoreInteractions(streamRequest);
      verifyNoMoreInteractions(entityStream);
      verifyNoMoreInteractions(readHandle);
    }



    //todo - test with callback in construction




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


    private static class TestSinglePartMIMEReaderCallbackImpl implements SinglePartMIMEReaderCallback {

        final MultiPartMIMEReaderCallback _topLevelCallback;
        final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
        final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
        Map<String, String> _headers;
        ByteString _finishedData = null;

        TestSinglePartMIMEReaderCallbackImpl(final MultiPartMIMEReaderCallback topLevelCallback, final
        MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
            _topLevelCallback = topLevelCallback;
            _singlePartMIMEReader = singlePartMIMEReader;
            _headers = singlePartMIMEReader.getHeaders();
        }

        @Override
        public void onPartDataAvailable(ByteString b) {

            try {
                _byteArrayOutputStream.write(b.copyBytes());
            } catch (IOException ioException) {
                onStreamError(ioException);
            }
            _singlePartMIMEReader.requestPartData();
        }

        @Override
        public void onFinished() {
            _finishedData = ByteString.copy(_byteArrayOutputStream.toByteArray());
        }

        //Delegate to the top level for now for these two
        @Override
        public void onAbandoned() {
            //This will end up failing the test.
            _topLevelCallback.onAbandoned();
        }

        @Override
        public void onStreamError(Throwable e) {
            Assert.fail();
        }

    }

    private static class TestMultiPartMIMEReaderCallbackImpl implements MultiPartMIMEReaderCallback {

        final Callback<Integer> _r2callback;
        final List<TestSinglePartMIMEReaderCallbackImpl> _singlePartMIMEReaderCallbacks =
                new ArrayList<TestSinglePartMIMEReaderCallbackImpl>();

        @Override
        public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader) {
            TestSinglePartMIMEReaderCallbackImpl singlePartMIMEReaderCallback = new TestSinglePartMIMEReaderCallbackImpl(this, singleParMIMEReader);
            singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
            _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
            singleParMIMEReader.requestPartData();
        }

        @Override
        public void onFinished() {
            _r2callback.onSuccess(200);
        }

        @Override
        public void onAbandoned() {
            RestException restException = new RestException(RestStatus.responseForStatus(406, "Not Acceptable"));
            _r2callback.onError(restException);
        }

        @Override
        public void onStreamError(Throwable e) {
            Assert.fail();

        }

        TestMultiPartMIMEReaderCallbackImpl(final Callback<Integer> r2callback) {
            _r2callback = r2callback;
        }
    }

}
