package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.EntityStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.multipart.DataSources.*;
import static org.mockito.Mockito.when;


/**
 * Created by kvidhani on 7/13/15.
 */
public class TestMultiPartMIMEIntegrationReaderExceptions {


  /*
  @Test
  public void basicReadSandbox()
  {
    StreamRequest streamRequest = Mockito.mock(StreamRequest.class);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn()
  }

  @Test
  public void with_arguments(){

    Comparable c= Mockito.mock(Comparable.class);
    when(c.compareTo("Test")).thenReturn(1);
    Assert.assertEquals(1, c.compareTo("Test"));
  }
  */

  private static final Logger log = LoggerFactory.getLogger(TestMultiPartMIMEIntegrationReader.class);
/*

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

*/

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
    //final VariableByteStringWriter variableByteStringWriter = new VariableByteStringWriter(requestPayload, chunkSize);
    //executeRequestAndAssert(variableByteStringWriter, multiPartMimeBody);
    executeRequestAndAssert(multiPartMimeBody);

  }

/*

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
*/

  ///////////////////////////////////////////////////////////////////////////////////////

  private void executeRequestAndAssert(final MimeMultipart mimeMultipart) throws Exception {

    //final EntityStream entityStream = EntityStreams.newEntityStream(dataSourceWriter);
    final EntityStream entityStream = Mockito.mock(EntityStream.class);
    //final StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, SERVER_URI));

    //We add additional parameters since MIME supports this and we want to make sure we can still extract boundary
    //properly.
    final String contentTypeHeader = mimeMultipart.getContentType() + ";somecustomparameter=somecustomvalue"
        + ";anothercustomparameter=anothercustomvalue";
    //StreamRequest request = builder.setMethod("POST").setHeader(HEADER_CONTENT_TYPE, contentTypeHeader).build(
     //   entityStream);

    StreamRequest streamRequest = Mockito.mock(StreamRequest.class);
    //todo number of times? and assert it happened that many times?
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);

    Mockito.verify(streamRequest, Mockito.times(1));
    //final AtomicInteger status = new AtomicInteger(-1);
    //final CountDownLatch latch = new CountDownLatch(1);
    //Callback<StreamResponse> callback = expectSuccessCallback(latch, status, new HashMap<String, String>());
    //_client.streamRequest(request, callback);
    //latch.await(60000, TimeUnit.MILLISECONDS);
    //Assert.assertEquals(status.get(), RestStatus.OK);


    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
    //final TestMultiPartMIMEReaderCallbackImpl multiPartMIMEReaderCallback  = new TestMultiPartMIMEReaderCallbackImpl();
    //reader.registerReaderCallback(multiPartMIMEReaderCallback);

/*
    List<TestSinglePartMIMEReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _mimeServerRequestHandler._currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
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
    }*/
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

    //final Callback<StreamResponse> _r2callback;
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
      //_r2callback.onSuccess(Messages.toStreamResponse(response));
    }

    @Override
    public void onAbandoned() {
      RestException restException = new RestException(RestStatus.responseForStatus(406, "Not Acceptable"));
      //_r2callback.onError(restException);
    }

    @Override
    public void onStreamError(Throwable e) {
      RestException restException = new RestException(RestStatus.responseForError(400, e));
      //_r2callback.onError(restException);
    }

    TestMultiPartMIMEReaderCallbackImpl() {
      //_r2callback = r2callback;
    }
  }

  /*
        MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(request);
        _currentMultiPartMIMEReaderCallback  = new TestMultiPartMIMEReaderCallbackImpl(callback);
        reader.registerReaderCallback(_currentMultiPartMIMEReaderCallback);
      }
      catch (IllegalMimeFormatException illegalMimeFormatException)
      {
        RestException restException = new RestException(RestStatus.responseForError(400, illegalMimeFormatException));
        callback.onError(restException);
      }
      */

}
