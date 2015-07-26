package com.linkedin.multipart;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.multipart.integ.AbstractMultiPartMIMEIntegrationStreamTest;
import com.linkedin.multipart.reader.exceptions.IllegalMimeFormatException;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.*;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.mail.BodyPart;
import javax.mail.Header;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static com.linkedin.multipart.DataSources.*;



/**
 * Created by kvidhani on 7/11/15.
 */
public class TestMultiPartMIMEIntegrationReaderAbandon extends AbstractMultiPartMIMEIntegrationStreamTest {

  private static final URI SERVER_URI = URI.create("/pegasusAbandonServer");
  private MimeServerRequestAbandonHandler _mimeServerRequestAbandonHandler;
  private static final Logger log = LoggerFactory.getLogger(TestMultiPartMIMEIntegrationReaderAbandon.class);
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

  @Override
  protected TransportDispatcher getTransportDispatcher() {
    _mimeServerRequestAbandonHandler = new MimeServerRequestAbandonHandler();
    return new TransportDispatcherBuilder().addStreamHandler(SERVER_URI, _mimeServerRequestAbandonHandler).build();
  }

  @Override
  protected Map<String, String> getClientProperties() {
    Map<String, String> clientProperties = new HashMap<String, String>();
    clientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "9000000");
    return clientProperties;
  }

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
        _mimeServerRequestAbandonHandler._testMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 0);
  }

  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testAbandonAll(final int chunkSize, final List<MimeBodyPart> bodyPartList)
      throws Exception {
    //Execute the request and verify the correct header came back to ensure the server took the proper abandon actions.
    executeRequestWithAbandonStrategy(chunkSize, bodyPartList, TOP_ALL, "onAbandoned");

    //Top level abandons all
    List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _mimeServerRequestAbandonHandler._testMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 0);
  }

  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testSinglePartialTopRemaining(final int chunkSize, final List<MimeBodyPart> bodyPartList)
      throws Exception {
    //Execute the request, verify the correct header came back to ensure the server took the proper abandon actions
    //and return the payload so we can assert deeper.
    MimeMultipart mimeMultipart =
        executeRequestWithAbandonStrategy(chunkSize, bodyPartList, SINGLE_PARTIAL_TOP_REMAINING, "onAbandoned");

    //Single part abandons the first 6 then the top level abandons all of remaining
    List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _mimeServerRequestAbandonHandler._testMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;

    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 6);

    for (int i = 0; i < singlePartMIMEReaderCallbacks.size(); i++) {
      //Actual 
      final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
      //Expected 
      final BodyPart currentExpectedPart = mimeMultipart.getBodyPart(i);

      //Construct expected headers and verify they match 
      final Map<String, String> expectedHeaders = new HashMap<String, String>();
      final Enumeration allHeaders = currentExpectedPart.getAllHeaders();

      while (allHeaders.hasMoreElements()) {
        final Header header = (Header) allHeaders.nextElement();
        expectedHeaders.put(header.getName(), header.getValue());
      }

      Assert.assertEquals(currentCallback._headers, expectedHeaders);
      //Verify that the bodies are empty
      Assert.assertEquals(currentCallback._finishedData, ByteString.empty());
    }
  }

  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testSingleAlternateTopRemaining(final int chunkSize, final List<MimeBodyPart> bodyPartList)
      throws Exception {
    //Execute the request, verify the correct header came back to ensure the server took the proper abandon actions
    //and return the payload so we can assert deeper.
    MimeMultipart mimeMultipart =
        executeRequestWithAbandonStrategy(chunkSize, bodyPartList, SINGLE_ALTERNATE_TOP_REMAINING, "onAbandoned");

    //Single part alternates between consumption and abandoning the first 6 parts, then top level abandons all of remaining.
    //This means that parts 0, 2, 4 will be consumed and parts 1, 3, 5 will be abandoned.
    List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _mimeServerRequestAbandonHandler._testMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;

    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 6);

    //First the consumed
    for (int i = 0; i < singlePartMIMEReaderCallbacks.size(); i=i+2) {
      //Actual 
      final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
      //Expected 
      final BodyPart currentExpectedPart = mimeMultipart.getBodyPart(i);

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
      final BodyPart currentExpectedPart = mimeMultipart.getBodyPart(i);

      //Construct expected headers and verify they match 
      final Map<String, String> expectedHeaders = new HashMap<String, String>();
      final Enumeration allHeaders = currentExpectedPart.getAllHeaders();

      while (allHeaders.hasMoreElements()) {
        final Header header = (Header) allHeaders.nextElement();
        expectedHeaders.put(header.getName(), header.getValue());
      }

      Assert.assertEquals(currentCallback._headers, expectedHeaders);
      //Verify that the bodies are empty
      Assert.assertEquals(currentCallback._finishedData, ByteString.empty());
    }
  }

  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testSingleAll(final int chunkSize, final List<MimeBodyPart> bodyPartList)
      throws Exception {
    //Execute the request, verify the correct header came back to ensure the server took the proper abandon actions
    //and return the payload so we can assert deeper.
    MimeMultipart mimeMultipart =
        executeRequestWithAbandonStrategy(chunkSize, bodyPartList, SINGLE_ALL, "onFinished");

    //Single part abandons all, one by one
    List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _mimeServerRequestAbandonHandler._testMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;

    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 12);

    //Verify everything was abandoned
    for (int i = 0; i < singlePartMIMEReaderCallbacks.size(); i++) {
      //Actual 
      final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
      //Expected 
      final BodyPart currentExpectedPart = mimeMultipart.getBodyPart(i);

      //Construct expected headers and verify they match 
      final Map<String, String> expectedHeaders = new HashMap<String, String>();
      final Enumeration allHeaders = currentExpectedPart.getAllHeaders();

      while (allHeaders.hasMoreElements()) {
        final Header header = (Header) allHeaders.nextElement();
        expectedHeaders.put(header.getName(), header.getValue());
      }

      Assert.assertEquals(currentCallback._headers, expectedHeaders);
      //Verify that the bodies are empty
      Assert.assertEquals(currentCallback._finishedData, ByteString.empty());
    }
  }


  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testSingleAlternate(final int chunkSize, final List<MimeBodyPart> bodyPartList)
      throws Exception {
    //Execute the request, verify the correct header came back to ensure the server took the proper abandon actions
    //and return the payload so we can assert deeper.
    MimeMultipart mimeMultipart =
        executeRequestWithAbandonStrategy(chunkSize, bodyPartList, SINGLE_ALTERNATE, "onFinished");

    //Single part alternates between consumption and abandoning for all 12 parts.
    //This means that parts 0, 2, 4, etc.. will be consumed and parts 1, 3, 5, etc... will be abandoned.
    List<SinglePartMIMEAbandonReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _mimeServerRequestAbandonHandler._testMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;

    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 12);

    //First the consumed
    for (int i = 0; i < singlePartMIMEReaderCallbacks.size(); i=i+2) {
      //Actual 
      final SinglePartMIMEAbandonReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
      //Expected 
      final BodyPart currentExpectedPart = mimeMultipart.getBodyPart(i);

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
      final BodyPart currentExpectedPart = mimeMultipart.getBodyPart(i);

      //Construct expected headers and verify they match 
      final Map<String, String> expectedHeaders = new HashMap<String, String>();
      final Enumeration allHeaders = currentExpectedPart.getAllHeaders();

      while (allHeaders.hasMoreElements()) {
        final Header header = (Header) allHeaders.nextElement();
        expectedHeaders.put(header.getName(), header.getValue());
      }

      Assert.assertEquals(currentCallback._headers, expectedHeaders);
      //Verify that the bodies are empty
      Assert.assertEquals(currentCallback._finishedData, ByteString.empty());
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  private MimeMultipart executeRequestWithAbandonStrategy(final int chunkSize, final List<MimeBodyPart> bodyPartList,
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
    final VariableByteStringWriter variableByteStringWriter = new VariableByteStringWriter(requestPayload, chunkSize);
    //executeRequestAndAssert(variableByteStringWriter, multiPartMimeBody);

    final EntityStream entityStream = EntityStreams.newEntityStream(variableByteStringWriter);
    final StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, SERVER_URI));

    StreamRequest request = builder.setMethod("POST").setHeader(HEADER_CONTENT_TYPE, multiPartMimeBody.getContentType())
        .setHeader(ABANDON_HEADER, abandonStrategy).build(entityStream);

    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    final Map<String, String> responseHeaders = new HashMap<String, String>();
    Callback<StreamResponse> callback = expectSuccessCallback(latch, status, responseHeaders);
    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), RestStatus.OK);
    Assert.assertEquals(responseHeaders.get(ABANDON_HEADER), serverHeaderPrefix + abandonStrategy);
    return multiPartMimeBody;
  }

  private static class SinglePartMIMEAbandonReaderCallbackImpl implements SinglePartMIMEReaderCallback {

    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    static String _abandonValue;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData = ByteString.empty();
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

    final Callback<StreamResponse> _r2callback;
    final String _abandonValue;
    final MultiPartMIMEReader _reader;
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
      RestResponse response =
          new RestResponseBuilder().setStatus(RestStatus.OK).setHeader(ABANDON_HEADER, "onFinished" + _abandonValue).build();
      _r2callback.onSuccess(Messages.toStreamResponse(response));
    }

    @Override
    public void onAbandoned() {
      //Happens for TOP_ALL, SINGLE_PARTIAL_TOP_REMAINING and SINGLE_ALTERNATE_TOP_REMAINING
      RestResponse response =
          new RestResponseBuilder().setStatus(RestStatus.OK).setHeader(ABANDON_HEADER, "onAbandoned" + _abandonValue).build();
      _r2callback.onSuccess(Messages.toStreamResponse(response));
    }

    @Override
    public void onStreamError(Throwable e) {
      RestException restException = new RestException(RestStatus.responseForError(400, e));
      _r2callback.onError(restException);
    }

    MultiPartMIMEAbandonReaderCallbackImpl(final Callback<StreamResponse> r2callback, final String abandonValue,
        final MultiPartMIMEReader reader) {
      _r2callback = r2callback;
      _abandonValue = abandonValue;
      _reader = reader;
      SinglePartMIMEAbandonReaderCallbackImpl._abandonValue = _abandonValue;
    }
  }

  private static class MimeServerRequestAbandonHandler implements StreamRequestHandler {
    private MultiPartMIMEAbandonReaderCallbackImpl _testMultiPartMIMEReaderCallback;

    MimeServerRequestAbandonHandler() {
    }

    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext,
        final Callback<StreamResponse> callback) {
      try {
        //todo assert the request has multipart content type
        MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(request);
        final String shouldAbandonValue = request.getHeader(ABANDON_HEADER);
        _testMultiPartMIMEReaderCallback =
            new MultiPartMIMEAbandonReaderCallbackImpl(callback, shouldAbandonValue, reader);
        reader.registerReaderCallback(_testMultiPartMIMEReaderCallback);
      } catch (IllegalMimeFormatException illegalMimeFormatException) {
        RestException restException = new RestException(RestStatus.responseForError(400, illegalMimeFormatException));
        callback.onError(restException);
      }
    }
  }
}
