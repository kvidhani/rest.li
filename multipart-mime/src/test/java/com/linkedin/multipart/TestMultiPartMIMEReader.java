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
import javax.mail.internet.ContentType;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import test.r2.integ.AbstractStreamTest;



//Areas to test:

//the writer
//-various data sources, input stream data source
//read using javax mail

//the reader
// write using javax and read using our stuff
// make sure all areas of the code are exercised

//reader AND writer together to stream between the two
//you can use the data sources from your sync MIME rb

//chaining

//Exceptions
// - cancellations, aborts, exceptions in chain scenarios
//poorly formatted data on reader stide

//Thread safety stuff and race conditions

//multipart mime utils

//Stream request, stream response builder

//CHANGE R2 SO THAT IT ONLY PROVIDES VERY FEW BYTES ON WRITE?

//Note that we use javax.mail's ability to create multipart mime requests to verify the integrity of our RFC implementation.
public class TestMultiPartMIMEReader extends AbstractStreamTest {

  private static final URI SERVER_URI = URI.create("/pegasusMimeServer");
  private MimeServerRequestHandler _mimeServerRequestHandler;
  private static final Logger log = LoggerFactory.getLogger(TestMultiPartMIMEReader.class);
  private static final String HEADER_CONTENT_TYPE = "Content-Type";

  private static MimeBodyPart _smallDataSource;
  private static MimeBodyPart _largeDataSource;


  @BeforeClass
  public void dataSourceSetup() throws Exception {

    //Small body.
    {
      final String body = "A tiny body";
      MimeBodyPart dataPart = new MimeBodyPart();
      ContentType contentType = new ContentType("text/plain");
      dataPart.setContent(body, contentType.getBaseType());
      dataPart.setHeader(HEADER_CONTENT_TYPE, "text/plain");
      _smallDataSource = dataPart;
    }

    //Large body. Something bigger then the size of the boundary.
    {
      final String body = "Has at possim tritani laoreet, vis te meis verear. Vel no vero quando oblique, "
          + "eu blandit placerat nec, vide facilisi recusabo nec te. Veri labitur sensibus eum id. Quo omnis "
          + "putant erroribus ad, nonumes copiosae percipit in qui, id cibo meis clita pri. An brute "
          + "mundi quaerendum duo, eu aliquip facilisis sea, eruditi invidunt dissentiunt eos ea.";
      MimeBodyPart dataPart = new MimeBodyPart();
      ContentType contentType = new ContentType("text/plain");
      dataPart.setContent(body, contentType.getBaseType());
      dataPart.setHeader(HEADER_CONTENT_TYPE, "text/plain");
      dataPart.setHeader("SomeCustomHeader", "SomeCustomValue");
      _largeDataSource = dataPart;
    }

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

  @DataProvider(name = "smallSingleBodyDataSource")
  public Object[][] smallSingleBodyDataSource() throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    multiPartMimeBody.addBodyPart(_smallDataSource);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());

    return new Object[][] {
            { new VariableByteStringWriter(requestPayload, 1), multiPartMimeBody  },
            { new VariableByteStringWriter(requestPayload, R2Constants.DEFAULT_DATA_CHUNK_SIZE), multiPartMimeBody }
    };
  }

  @Test(dataProvider = "smallSingleBodyDataSource")
  public void testSmallSingleBody(final Writer dataSourceWriter, final MimeMultipart mimeMultipart) throws Exception
  {
    executeRequestAndAssert(dataSourceWriter, mimeMultipart);
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  @DataProvider(name = "largeSingleBodyDataSource")
     public Object[][] largeSingleBodyDataSource() throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    multiPartMimeBody.addBodyPart(_largeDataSource);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());

    return new Object[][] {
            { new VariableByteStringWriter(requestPayload, 1), multiPartMimeBody  },
            { new VariableByteStringWriter(requestPayload, R2Constants.DEFAULT_DATA_CHUNK_SIZE), multiPartMimeBody }
    };
  }

  @Test(dataProvider = "largeSingleBodyDataSource")
  public void testLargeSingleBody(final Writer dataSourceWriter, final MimeMultipart mimeMultipart) throws Exception
  {
    executeRequestAndAssert(dataSourceWriter, mimeMultipart);
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  @DataProvider(name = "multipleBodiesDataSource")
  public Object[][] multipleBodiesDataSource() throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    multiPartMimeBody.addBodyPart(_largeDataSource);
    multiPartMimeBody.addBodyPart(_smallDataSource);
    multiPartMimeBody.addBodyPart(_largeDataSource);
    multiPartMimeBody.addBodyPart(_smallDataSource);
    multiPartMimeBody.addBodyPart(_largeDataSource);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());

    return new Object[][] {
            { new VariableByteStringWriter(requestPayload, 1), multiPartMimeBody  },
            { new VariableByteStringWriter(requestPayload, R2Constants.DEFAULT_DATA_CHUNK_SIZE), multiPartMimeBody }
    };
  }

  @Test(dataProvider = "multipleBodiesDataSource")
  public void testMultipleBodies(final Writer dataSourceWriter, final MimeMultipart mimeMultipart) throws Exception
  {
    executeRequestAndAssert(dataSourceWriter, mimeMultipart);
  }

  ///////////////////////////////////////////////////////////////////////////////////////


  private void executeRequestAndAssert(final Writer dataSourceWriter, final MimeMultipart mimeMultipart) throws Exception {

    final EntityStream entityStream = EntityStreams.newEntityStream(dataSourceWriter);
    final StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, SERVER_URI));
    StreamRequest request = builder.setMethod("POST").setHeader(HEADER_CONTENT_TYPE, mimeMultipart.getContentType()).build(
            entityStream);

    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = expectSuccessCallback(latch, status);
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
      Assert.assertEquals(new String(currentCallback._finishedData.copyBytes()), currentExpectedPart.getContent());
    }
  }



  //todo epligous and prologus and all the stuff
  //test with a bigger body that's bigger then the boundary size
  //test with all sorts of payloads
  //test aborting single parts and all parts
  //paramteriez on single byte, individual parts on write and entire too
  //todo try a body part with bytes too



  private static Callback<StreamResponse> expectSuccessCallback(final CountDownLatch latch, final AtomicInteger status)
  {
    return new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        latch.countDown();
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        status.set(result.getStatus());
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
    int partCounter = 0;

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
      //This will end up failing the test.
      _topLevelCallback.onStreamError(e);
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