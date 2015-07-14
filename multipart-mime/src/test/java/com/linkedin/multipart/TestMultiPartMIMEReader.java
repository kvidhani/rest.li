package com.linkedin.multipart;

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
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.Writer;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.mail.BodyPart;
import javax.mail.Header;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;



//Areas to test (DOUBLE CHECK ALL OF THIS IS DONE BEFORE RB):
//look for remaining todos in tests

//the writer
//-various data sources, input stream data source
//read using javax mail


//the reader
// write using javax and read using our stuff
// make sure all areas of the code are exercised
//todo epligous and prologus and all the stuff
//exceptions and abortions and what not...
//verify you can retrieve the preamble
//exception tests should cover all possible exceptions in multipart mime reader


//reader AND writer together to stream between the two
//Make sure you do server sending back and client processing too
//you can use the data sources from your sync MIME rb

//chaining
//exceptions thrown in the middle of chaining
//abandoning parts coming in from a chain

//Exceptions
// - cancellations, aborts, exceptions in chain scenarios
//poorly formatted data on reader stide

//Thread safety stuff and race conditions

//multipart mime utils

//input stream class you wrote testing

//Stream request, stream response builder

//CHANGE R2 SO THAT IT ONLY PROVIDES VERY FEW BYTES ON WRITE?

//consider writing unit tests? is that feasible?

//todo open a jira so that we can consider tirmming folded headers after they are parsed in
//Note that we use javax.mail's ability to create multipart mime requests to verify the integrity of our RFC implementation.
public class TestMultiPartMIMEReader extends AbstractMultiPartMIMEStreamTest {

  private static final URI SERVER_URI = URI.create("/pegasusMimeServer");
  private MimeServerRequestHandler _mimeServerRequestHandler;
  private static final Logger log = LoggerFactory.getLogger(TestMultiPartMIMEReader.class);

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
    final VariableByteStringWriter variableByteStringWriter = new VariableByteStringWriter(requestPayload, chunkSize);
    executeRequestAndAssert(variableByteStringWriter, multiPartMimeBody);
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
    final VariableByteStringWriter variableByteStringWriter = new VariableByteStringWriter(requestPayload, chunkSize);
    executeRequestAndAssert(variableByteStringWriter, multiPartMimeBody);
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
    final VariableByteStringWriter variableByteStringWriter = new VariableByteStringWriter(requestPayload, chunkSize);
    executeRequestAndAssert(variableByteStringWriter, multiPartMimeBody);
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
    final VariableByteStringWriter variableByteStringWriter = new VariableByteStringWriter(requestPayload, chunkSize);
    executeRequestAndAssert(variableByteStringWriter, multiPartMimeBody);
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
    final VariableByteStringWriter variableByteStringWriter = new VariableByteStringWriter(requestPayload, chunkSize);
    executeRequestAndAssert(variableByteStringWriter, multiPartMimeBody);
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
    final VariableByteStringWriter variableByteStringWriter = new VariableByteStringWriter(requestPayload, chunkSize);
    executeRequestAndAssert(variableByteStringWriter, multiPartMimeBody);
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  private void executeRequestAndAssert(final Writer dataSourceWriter, final MimeMultipart mimeMultipart) throws Exception {

    final EntityStream entityStream = EntityStreams.newEntityStream(dataSourceWriter);
    final StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, SERVER_URI));

    //We add additional parameters since MIME supports this and we want to make sure we can still extract boundary
    //properly.
    final String contentTypeHeader = mimeMultipart.getContentType() + ";somecustomparameter=somecustomvalue"
        + ";anothercustomparameter=anothercustomvalue";
    StreamRequest request = builder.setMethod("POST").setHeader(HEADER_CONTENT_TYPE, contentTypeHeader).build(
            entityStream);

    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = expectSuccessCallback(latch, status, new HashMap<String, String>());
    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), RestStatus.OK);

    List<TestSinglePartMIMEReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _mimeServerRequestHandler._testMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
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
    {}

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