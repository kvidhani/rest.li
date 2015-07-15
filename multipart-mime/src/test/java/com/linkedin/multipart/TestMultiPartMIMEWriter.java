package com.linkedin.multipart;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.multipart.reader.exceptions.IllegalMimeFormatException;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.mail.internet.ContentType;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import test.r2.integ.AbstractStreamTest;


/**
 * Created by kvidhani on 7/7/15.
 */
//Using Javax.mail on the server side to verify the integrity of our RFC implementation of the MultiPartMIMEWriter

public class TestMultiPartMIMEWriter extends AbstractStreamTest {

  private static final URI SERVER_URI = URI.create("/javaxMimeServer");
  private MimeServerRequestHandler _mimeServerRequestHandler;
  private static final Logger log = LoggerFactory.getLogger(TestMultiPartMIMEIntegrationReader.class);
  private static final String HEADER_CONTENT_TYPE = "Content-Type";

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
    clientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "300000");
    return clientProperties;
  }

  @Mock
  MultiPartMIMEDataSource a;

  @Mock
  MultiPartMIMEDataSource b;


  @BeforeTest
  public void testSetup() {

    MultiPartMIMEWriter.MultiPartMIMEWriterBuilder multiPartMIMEWriterBuilder =
        new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder("some preamble", "some epilogue");


    {
      final String body = "A tiny body";
      final Map<String, String> partMap = new HashMap<String, String>();
      partMap.put(HEADER_CONTENT_TYPE, "text/plain");
      partMap.put("SomeCustomHeader", "SomeCustomValue");

      final ByteArrayInputStream byteArrayInputStreamA = new ByteArrayInputStream(body.getBytes());
      MultiPartMIMEInputStream dataSourceA =
          new MultiPartMIMEInputStream.Builder(byteArrayInputStreamA, new ScheduledThreadPoolExecutor(5), partMap)
              .withMaximumBlockingTime(100).withWriteChunkSize(1).build();
      multiPartMIMEWriterBuilder.appendDataSource(dataSourceA);
    }
    {
      final String body = "Has at possim tritani laoreet, vis te meis verear. Vel no vero quando oblique, eu blandit placerat nec, vide facilisi recusabo nec te. Veri labitur sensibus eum id. Quo omnis "
        + "putant erroribus ad, nonumes copiosae percipit in qui, id cibo meis clita pri. An brute mundi quaerendum duo, eu aliquip facilisis sea, eruditi invidunt dissentiunt eos ea.";
      final ByteArrayInputStream byteArrayInputStreamB = new ByteArrayInputStream(body.getBytes());
      final Map<String, String> partMap = new HashMap<String, String>();
      partMap.put(HEADER_CONTENT_TYPE, "text/plain");
      partMap.put("SomeCustomHeader", "SomeCustomValue");

      MultiPartMIMEInputStream dataSourceB =
          new MultiPartMIMEInputStream.Builder(byteArrayInputStreamB, new ScheduledThreadPoolExecutor(5), partMap)
              .withMaximumBlockingTime(100).withWriteChunkSize(1).build();
      multiPartMIMEWriterBuilder.appendDataSource(dataSourceB);
    }

    MultiPartMIMEWriter writer = multiPartMIMEWriterBuilder.build();

    final EntityStreams entityStreams = Mockito.mock(EntityStreams.class);


    //final WriteHandle writeHandle = Mockito.mock(WriteHandle.class);
    //dataSource.onInit(writeHandle);

    //when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);
    //when(streamRequest.getEntityStream()).thenReturn(entityStream);


    //do all the other bodies
    //body1
    //body2

    //test only up until the composite writer is created
  }



  @Test
  public void testSimpleReaderRequest() throws Exception
  {
    //Create a simple multi part mime request with just one part
    MimeMultipart multi = new MimeMultipart();
    MimeBodyPart dataPart = new MimeBodyPart();
    ContentType contentType = new ContentType("text/plain");
    dataPart.setContent("Some bytes for some body", contentType.getBaseType());
    dataPart.setHeader(HEADER_CONTENT_TYPE, "test/plain");
    multi.addBodyPart(dataPart);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multi.writeTo(byteArrayOutputStream);
    log.info("The request we are sending is: " + new String(byteArrayOutputStream.toByteArray()));
    final ByteStringWriter byteStringWriter = new ByteStringWriter(ByteString.copy(byteArrayOutputStream.toByteArray()));
    EntityStream entityStream = EntityStreams.newEntityStream(byteStringWriter);
    StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, SERVER_URI));
    StreamRequest request = builder.setMethod("POST").setHeader(HEADER_CONTENT_TYPE, multi.getContentType()) .build(entityStream);

    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = expectSuccessCallback(latch, status);
    _client.streamRequest(request, callback);
    latch.await(60000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), RestStatus.OK);
    //BytesReader reader = _checkRequestHandler.getReader();
    //Assert.assertNotNull(reader);
    //Assert.assertEquals(totalBytes, reader.getTotalBytes());
    //Assert.assertTrue(reader.allBytesCorrect());
    //todo make sure all callbacks are invoked
  }


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
    int partCounter = 0;

    TestSinglePartMIMEReaderCallbackImpl(final MultiPartMIMEReaderCallback topLevelCallback, final
    MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
      _topLevelCallback = topLevelCallback;
      _singlePartMIMEReader = singlePartMIMEReader;
      log.info("The headers for the current part " + partCounter + " are: ");
      log.info(singlePartMIMEReader.getHeaders().toString());
    }

    @Override
    public void onPartDataAvailable(ByteString b) {
      log.info("Just received " + b.length() + " byte(s) on the single part reader callback for part number " + partCounter);
      _singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished() {
      log.info("Part " + partCounter++ + " is done!");
    }

    //Delegate to the top level for now for these two
    @Override
    public void onAbandoned() {
      _topLevelCallback.onAbandoned();
    }

    @Override
    public void onStreamError(Throwable e) {
      _topLevelCallback.onStreamError(e);
    }

  }

  private static class TestMultiPartMIMEReaderCallbackImpl implements MultiPartMIMEReaderCallback {

    final Callback<StreamResponse> _r2callback;

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader) {
      SinglePartMIMEReaderCallback singlePartMIMEReaderCallback = new TestSinglePartMIMEReaderCallbackImpl(this, singleParMIMEReader);
      singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
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
    private MultiPartMIMEReader _reader;

    MimeServerRequestHandler()
    {}

    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext, final Callback<StreamResponse> callback)
    {
      try {
        //todo assert the request has multipart content type
        _reader = MultiPartMIMEReader.createAndAcquireStream(request);
        final MultiPartMIMEReaderCallback testMultiPartMIMEReaderCallback = new TestMultiPartMIMEReaderCallbackImpl(callback);
        _reader.registerReaderCallback(testMultiPartMIMEReaderCallback);
      } catch (IllegalMimeFormatException illegalMimeFormatException) {
        RestException restException = new RestException(RestStatus.responseForError(400, illegalMimeFormatException));
        callback.onError(restException);      }
    }
  }



}
