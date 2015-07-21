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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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


/**
 * Created by kvidhani on 7/19/15.
 */
public class TestMultiPartMIMEReader {

    private static final Logger log = LoggerFactory.getLogger(TestMultiPartMIMEIntegrationReader.class);

    //We will have only one thread in our executor service so that when we submit tasks to write
    //data from the writer we do it in order.
    private static ScheduledExecutorService singleScheduledExecutorService;

    @BeforeTest
    public void setup() {
        singleScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterTest
    public void shutDown() {
        singleScheduledExecutorService.shutdownNow();
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
                { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _purelyEmptyBody }
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
        executeRequestAndAssert(requestPayload, chunkSize, multiPartMimeBody);
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
        executeRequestAndAssert(requestPayload, chunkSize, multiPartMimeBody);
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
        executeRequestAndAssert(requestPayload, chunkSize, multiPartMimeBody);
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
        executeRequestAndAssert(requestPayload, chunkSize, multiPartMimeBody);
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
        executeRequestAndAssert(requestPayload, chunkSize, multiPartMimeBody);
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
        if (epilogue != null) {
            //Javax mail does not support epilogue so we add it ourselves. Note that Javax mail throws in an extra CRLF
            //at the very end but this doesn't affect our tests.
            byteArrayOutputStream.write(epilogue.getBytes());
        }
        final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());
        executeRequestAndAssert(requestPayload, chunkSize, multiPartMimeBody);
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
                final int chunksRequested = (Integer)args[0]; //should always be 1

                for (int i = 0;i<chunksRequested; i++) {
                    ByteString clientData = variableByteStringViewer.onWritePossible();
                    if (clientData.equals(ByteString.empty())) {
                        reader.onDone();
                    }
                    else {
                        reader.onDataAvailable(clientData);
                    }
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
        MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
        TestMultiPartMIMEReaderCallbackImpl _testMultiPartMIMEReaderCallback =
                new TestMultiPartMIMEReaderCallbackImpl(callback);
        reader.registerReaderCallback(_testMultiPartMIMEReaderCallback);

        latch.await(60000, TimeUnit.MILLISECONDS);
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
            //Verify the body matches
            if(currentExpectedPart.getContent() instanceof byte[])
            {
                Assert.assertEquals(currentCallback._finishedData.copyBytes(), currentExpectedPart.getContent());
            } else {
                //Default is String
                Assert.assertEquals(new String(currentCallback._finishedData.copyBytes()), currentExpectedPart.getContent());
            }
        }


        //Assert mocks todo

        //entity stream
        //read handle
        //stream request

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
        ByteString _finishedData;
        static int partCounter = 0;

        TestSinglePartMIMEReaderCallbackImpl(final MultiPartMIMEReaderCallback topLevelCallback, final
        MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
            _topLevelCallback = topLevelCallback;
            _singlePartMIMEReader = singlePartMIMEReader;
            log.info("The headers for the current part " + partCounter + " are: ");
            log.info(singlePartMIMEReader.getHeaders().toString());
            _headers = singlePartMIMEReader.getHeaders();
        }

        @Override
        public void onPartDataAvailable(ByteString b) {
            log.info("Just received " + b.length() + " byte(s) on the single part reader callback for part number " + partCounter);
            try {
                _byteArrayOutputStream.write(b.copyBytes());
            } catch (IOException ioException) {
                onStreamError(ioException);
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
            //This will end up failing the test.
            _topLevelCallback.onAbandoned();
        }

        @Override
        public void onStreamError(Throwable e) {
            //MultiPartMIMEReader will end up calling onStreamError(e) on our top level callback
            //which will fail the test
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
            log.info("All parts finished for the request!");
            _r2callback.onSuccess(200);
        }

        @Override
        public void onAbandoned() {
            RestException restException = new RestException(RestStatus.responseForStatus(406, "Not Acceptable"));
            _r2callback.onError(restException);
        }

        @Override
        public void onStreamError(Throwable e) {
            RestException restException = new RestException(RestStatus.responseForError(400, e));
            _r2callback.onError(restException);

        }

        TestMultiPartMIMEReaderCallbackImpl(final Callback<Integer> r2callback) {
            _r2callback = r2callback;
        }
    }

}
