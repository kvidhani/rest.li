package com.linkedin.multipart;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.multipart.reader.exceptions.IllegalMimeFormatException;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Created by kvidhani on 7/22/15.
 */
//Full integration

public class TestMultiPartMIMEReaderWriter extends AbstractMultiPartMIMEIntegrationStreamTest {


  //todo resume here!
  //and then you have chaining
  //and clean up stream request/stream response
  //then code clena up
  //then rb



  private static ScheduledExecutorService scheduledExecutorService;
  private static final int TEST_CHUNK_SIZE = 4;
  private static final int TEST_TIMEOUT = 90000;




  private static final URI SERVER_URI = URI.create("/pegasusMimeServer");
    private MimeServerRequestHandler _mimeServerRequestHandler;
    private static final Logger log = LoggerFactory.getLogger(TestMultiPartMIMEIntegrationReader.class);



  byte[] _normalBodyData;
  Map<String, String> _normalBodyHeaders;

  byte[] _headerLessBodyData;

  Map<String, String> _bodyLessHeaders;

  MultiPartMIMEDataPartImpl _normalBody;
  MultiPartMIMEDataPartImpl _headerLessBody;
  MultiPartMIMEDataPartImpl _bodyLessBody;
  MultiPartMIMEDataPartImpl _purelyEmptyBody;

  @BeforeTest
  public void dataSourceSetup() {

    scheduledExecutorService = Executors.newScheduledThreadPool(10);

    _normalBodyData = "some normal body that is relatively small".getBytes();
    _normalBodyHeaders = new HashMap<String, String>();
    _normalBodyHeaders.put("simpleheader", "simplevalue");

    //Second body has no headers
    _headerLessBodyData = "a body without headers".getBytes();

    //Third body has only headers
    _bodyLessHeaders = new HashMap<String, String>();
    _normalBodyHeaders.put("header1", "value1");
    _normalBodyHeaders.put("header2", "value2");
    _normalBodyHeaders.put("header3", "value3");

    _normalBody =
        new MultiPartMIMEDataPartImpl(ByteString.copy(_normalBodyData), _normalBodyHeaders);

    _headerLessBody =
        new MultiPartMIMEDataPartImpl(ByteString.copy(_headerLessBodyData), Collections.<String, String>emptyMap());

    _bodyLessBody =
        new MultiPartMIMEDataPartImpl(ByteString.empty(), _bodyLessHeaders);

    _purelyEmptyBody =
        new MultiPartMIMEDataPartImpl(ByteString.empty(), Collections.<String, String>emptyMap());

  }



  @AfterTest
  public void shutDown() {
    scheduledExecutorService.shutdownNow();
  }




  @Override
    protected TransportDispatcher getTransportDispatcher()
    {
      _mimeServerRequestHandler = new MimeServerRequestHandler();
      return new TransportDispatcherBuilder()
          .addStreamHandler(SERVER_URI, _mimeServerRequestHandler)
          .build();
    }

    @Override
    protected Map<String, String> getClientProperties()
    {
      Map<String, String> clientProperties = new HashMap<String, String>();
      clientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "9000000");
      return clientProperties;
    }

    ///////////////////////////////////////////////////////////////////////////////////////

    @DataProvider(name = "eachSingleBodyDataSource")
    public Object[][] eachSingleBodyDataSource() throws Exception
    {
      return new Object[][] {
          { 1, _normalBody  },
          { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _normalBody },
          { 1, _headerLessBody  },
          { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _headerLessBody },
          { 1, _bodyLessBody  },
          { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _bodyLessBody },
          { 1, _purelyEmptyBody  },
          { R2Constants.DEFAULT_DATA_CHUNK_SIZE, _purelyEmptyBody },

      };
    }

    @Test(dataProvider = "eachSingleBodyDataSource")
    public void testEachSingleBodyDataSource(final int chunkSize, final MultiPartMIMEDataPartImpl bodyPart) throws Exception
    {

      final MultiPartMIMEInputStream inputStreamDataSource =
          new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(bodyPart._partData.copyBytes()), scheduledExecutorService, bodyPart._headers)
              .withWriteChunkSize(chunkSize)
              .build();

      final MultiPartMIMEWriter writer =
          new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder("some preamble", "").appendDataSource(inputStreamDataSource).build();

      executeRequestAndAssert(writer, ImmutableList.of(bodyPart));
    }


    @Test(dataProvider = "eachSingleBodyDataSource")
    public void testEachSingleBodyDataSourceMultipleTimes(final int chunkSize, final MultiPartMIMEDataPartImpl bodyPart) throws Exception
    {

      final List<MultiPartMIMEDataSource> dataSources = new ArrayList<MultiPartMIMEDataSource>();
    for (int i = 0;i<4;i++) {
      final MultiPartMIMEInputStream inputStreamDataSource =
          new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(bodyPart._partData.copyBytes()),
              scheduledExecutorService, bodyPart._headers).withWriteChunkSize(chunkSize).build();
      dataSources.add(inputStreamDataSource);
    }

      final MultiPartMIMEWriter writer =
          new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder("some preamble", "").appendDataSources(dataSources).build();

      executeRequestAndAssert(writer, ImmutableList.of(bodyPart, bodyPart, bodyPart, bodyPart));
    }


    ///////////////////////////////////////////////////////////////////////////////////////

    @DataProvider(name = "chunkSizes")
    public Object[][] chunkSizes() throws Exception
    {
      return new Object[][] {
          { 1},
          { R2Constants.DEFAULT_DATA_CHUNK_SIZE}
      };
    }

    @Test(dataProvider = "chunkSizes")
    public void testMultipleBodies(final int chunkSize) throws Exception
    {
      final MultiPartMIMEInputStream normalBodyInputStream =
          new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_normalBody._partData.copyBytes()), scheduledExecutorService, _normalBody._headers)
              .withWriteChunkSize(chunkSize)
              .build();

      final MultiPartMIMEInputStream headerLessBodyInputStream =
          new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_headerLessBody._partData.copyBytes()), scheduledExecutorService, _headerLessBody._headers)
              .withWriteChunkSize(chunkSize)
              .build();

      //Copying over empty ByteString, but let's keep things consistent.
      final MultiPartMIMEInputStream bodyLessBodyInputStream =
          new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyLessBody._partData.copyBytes()), scheduledExecutorService, _bodyLessBody._headers)
              .withWriteChunkSize(chunkSize)
              .build();

      final MultiPartMIMEInputStream purelyEmptyBodyInputStream =
          new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_purelyEmptyBody._partData.copyBytes()), scheduledExecutorService, _purelyEmptyBody._headers)
              .withWriteChunkSize(chunkSize)
              .build();

      final MultiPartMIMEWriter writer =
          new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder("some preamble", "").appendDataSource(normalBodyInputStream)
              .appendDataSource(headerLessBodyInputStream)
              .appendDataSource(bodyLessBodyInputStream)
              .appendDataSource(purelyEmptyBodyInputStream)
              .build();

      executeRequestAndAssert(writer, ImmutableList.of(_normalBody, _headerLessBody, _bodyLessBody, _purelyEmptyBody));
    }


  @Test
  public void testEmptyEnvelope() throws Exception
  {
    //Javax mail does not support this, hence we can only test this using our writer

    final MultiPartMIMEWriter writer =
        new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder("some preamble", "").build();

    executeRequestAndAssert(writer, Collections.<MultiPartMIMEDataPartImpl>emptyList());
  }




    ///////////////////////////////////////////////////////////////////////////////////////

  private void executeRequestAndAssert(final MultiPartMIMEWriter requestWriter, final List<MultiPartMIMEDataPartImpl> expectedParts) throws Exception {

    final StreamRequest multiPartMIMEStreamRequest =
        new MultiPartMIMEStreamRequestBuilder(Bootstrap.createHttpURI(PORT, SERVER_URI),
            "mixed",
            requestWriter,
            Collections.<String, String>emptyMap()).build();

    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = expectSuccessCallback(latch, status, new HashMap<String, String>());
    _client.streamRequest(multiPartMIMEStreamRequest, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), RestStatus.OK);


    List<TestSinglePartMIMEReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _mimeServerRequestHandler._testMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), expectedParts.size());
    for (int i = 0; i<singlePartMIMEReaderCallbacks.size();i++)
    {
      //Actual
      final TestSinglePartMIMEReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
      //Expected
      final MultiPartMIMEDataPartImpl currentExpectedPart = expectedParts.get(i);

      Assert.assertEquals(currentCallback._headers, currentExpectedPart.getPartHeaders());
      Assert.assertEquals(currentCallback._finishedData, currentExpectedPart.getPartData());
    }
  }

  private static class TestSinglePartMIMEReaderCallbackImpl implements SinglePartMIMEReaderCallback {

    final MultiPartMIMEReaderCallback _topLevelCallback;
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData = null;
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

    final Callback<StreamResponse> _r2callback;
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
      RestResponse response = RestStatus.responseForStatus(RestStatus.OK, "");
      _r2callback.onSuccess(Messages.toStreamResponse(response));
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

    TestMultiPartMIMEReaderCallbackImpl(final Callback<StreamResponse> r2callback) {
      _r2callback = r2callback;
    }
  }

  private static class MimeServerRequestHandler implements StreamRequestHandler
  {
    private TestMultiPartMIMEReaderCallbackImpl _testMultiPartMIMEReaderCallback;

    MimeServerRequestHandler()
    {
      System.out.println("A");
    }

    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, final Callback<StreamResponse> callback)
    {
      try
      {
        //todo assert the request has multipart content type
        MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(request);
        _testMultiPartMIMEReaderCallback  = new TestMultiPartMIMEReaderCallbackImpl(callback);
        reader.registerReaderCallback(_testMultiPartMIMEReaderCallback);
      }
      catch (IllegalMimeFormatException illegalMimeFormatException)
      {
        RestException restException = new RestException(RestStatus.responseForError(400, illegalMimeFormatException));
        callback.onError(restException);
      }
    }
  }


  }
