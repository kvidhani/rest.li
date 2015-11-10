/*
   Copyright (c) 2015 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.linkedin.multipart;


import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.multipart.exceptions.MultiPartIllegalFormatException;
import com.linkedin.multipart.utils.VariableByteStringWriter;
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

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.multipart.utils.MIMETestUtils.*;


/**
 * A series of integration tests that write multipart mime envelopes using Javax mail, and then use
 * {@link com.linkedin.multipart.MultiPartMIMEReader} on the server side to read and parse data out.
 *
 * @author Karim Vidhani
 */
public class TestMIMEIntegrationReader extends AbstractMIMEIntegrationStreamTest
{
  private static final URI SERVER_URI = URI.create("/pegasusMimeServer");
  private MimeServerRequestHandler _mimeServerRequestHandler;

  @Override
  protected TransportDispatcher getTransportDispatcher()
  {
    _mimeServerRequestHandler = new MimeServerRequestHandler();
    return new TransportDispatcherBuilder().addStreamHandler(SERVER_URI, _mimeServerRequestHandler).build();
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
    return new Object[][]{{1, _smallDataSource}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, _smallDataSource}, {1, _largeDataSource}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, _largeDataSource}, {1, _headerLessBody}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, _headerLessBody}, {1, _bodyLessBody}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, _bodyLessBody}, {1, _bytesBody}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, _bytesBody}, {1, _purelyEmptyBody}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, _purelyEmptyBody}};
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
    executeRequestAndAssert(trimTrailingCRLF(requestPayload), chunkSize, multiPartMimeBody);
  }

  @Test(dataProvider = "eachSingleBodyDataSource")
  public void testEachSingleBodyDataSourceMultipleTimes(final int chunkSize, final MimeBodyPart bodyPart)
      throws Exception
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

    //For this particular data source, we will use a variety of chunk sizes to cover all edge cases.
    //This is particularly useful due to the way we decompose ByteStrings when creating data
    //for our clients. Such chunk sizes allow us to make sure that our decomposing logic works as intended.
    final Object[][] multipleChunkPayloads = new Object[101][];
    for (int i = 0; i < 100; i++)
    {
      multipleChunkPayloads[i] = new Object[2];
      multipleChunkPayloads[i][0] = i + 1;
      multipleChunkPayloads[i][1] = bodyPartList;
    }
    multipleChunkPayloads[100] = new Object[2];
    multipleChunkPayloads[100][0] = R2Constants.DEFAULT_DATA_CHUNK_SIZE;
    multipleChunkPayloads[100][1] = bodyPartList;

    return multipleChunkPayloads;
  }

  @Test(dataProvider = "multipleNormalBodiesDataSource")
  public void testMultipleNormalBodiesDataSource(final int chunkSize, final List<MimeBodyPart> bodyPartList)
      throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList)
    {
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

    return new Object[][]{{1, bodyPartList}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList}};
  }

  @Test(dataProvider = "multipleAbnormalBodies")
  public void testMultipleAbnormalBodies(final int chunkSize, final List<MimeBodyPart> bodyPartList) throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList)
    {
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

    return new Object[][]{{1, bodyPartList}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList}};
  }

  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testAllTypesOfBodiesDataSource(final int chunkSize, final List<MimeBodyPart> bodyPartList)
      throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList)
    {
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

    return new Object[][]{{1, bodyPartList, null, null}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList, null, null}, {1, bodyPartList, "Some preamble", "Some epilogue"}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList, "Some preamble", "Some epilogue"}, {1, bodyPartList, "Some preamble", null}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList, "Some preamble", null}, {1, bodyPartList, null, "Some epilogue"}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList, null, "Some epilogue"}};
  }

  //Just test the preamble and epilogue here
  @Test(dataProvider = "preambleEpilogueDataSource")
  public void testPreambleAndEpilogue(final int chunkSize, final List<MimeBodyPart> bodyPartList, final String preamble,
      final String epilogue) throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList)
    {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    if (preamble != null)
    {
      multiPartMimeBody.setPreamble(preamble);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);

    final ByteString requestPayload;
    if (epilogue != null)
    {
      //Javax mail does not support epilogue so we add it ourselves (other then the CRLF following the final
      //boundary).
      byteArrayOutputStream.write(epilogue.getBytes());
      requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());
    }
    else
    {
      //Our test desired no epilogue.
      //Remove the CRLF introduced by javax mail at the end. We won't want a fake epilogue.
      requestPayload = trimTrailingCRLF(ByteString.copy(byteArrayOutputStream.toByteArray()));
    }

    executeRequestAndAssert(requestPayload, chunkSize, multiPartMimeBody);
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  private void executeRequestAndAssert(final ByteString requestPayload, final int chunkSize,
      final MimeMultipart mimeMultipart) throws Exception
  {
    final VariableByteStringWriter variableByteStringWriter = new VariableByteStringWriter(requestPayload, chunkSize);

    final EntityStream entityStream = EntityStreams.newEntityStream(variableByteStringWriter);
    final StreamRequestBuilder builder = new StreamRequestBuilder(Bootstrap.createHttpURI(PORT, SERVER_URI));

    //We add additional parameters since MIME supports this and we want to make sure we can still extract boundary
    //properly.
    final String contentTypeHeader = mimeMultipart.getContentType() + ";somecustomparameter=somecustomvalue"
        + ";anothercustomparameter=anothercustomvalue";
    StreamRequest request =
        builder.setMethod("POST").setHeader(HEADER_CONTENT_TYPE, contentTypeHeader).build(entityStream);

    final AtomicInteger status = new AtomicInteger(-1);
    final CountDownLatch latch = new CountDownLatch(1);
    Callback<StreamResponse> callback = expectSuccessCallback(latch, status, new HashMap<String, String>());
    _client.streamRequest(request, callback);
    latch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);
    Assert.assertEquals(status.get(), RestStatus.OK);

    List<SinglePartMIMEReaderCallbackImpl> singlePartMIMEReaderCallbacks =
        _mimeServerRequestHandler._testMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks;
    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), mimeMultipart.getCount());
    for (int i = 0; i < singlePartMIMEReaderCallbacks.size(); i++)
    {
      //Actual
      final SinglePartMIMEReaderCallbackImpl currentCallback = singlePartMIMEReaderCallbacks.get(i);
      //Expected
      final BodyPart currentExpectedPart = mimeMultipart.getBodyPart(i);

      //Construct expected headers and verify they match
      final Map<String, String> expectedHeaders = new HashMap<String, String>();
      @SuppressWarnings("unchecked")
      final Enumeration<Header> allHeaders = currentExpectedPart.getAllHeaders();
      while (allHeaders.hasMoreElements())
      {
        final Header header = allHeaders.nextElement();
        expectedHeaders.put(header.getName(), header.getValue());
      }
      Assert.assertEquals(currentCallback._headers, expectedHeaders);
      //Verify the body matches
      if (currentExpectedPart.getContent() instanceof byte[])
      {
        Assert.assertEquals(currentCallback._finishedData.copyBytes(), currentExpectedPart.getContent());
      }
      else
      {
        //Default is String
        Assert.assertEquals(new String(currentCallback._finishedData.copyBytes()), currentExpectedPart.getContent());
      }
    }
  }

  private static class SinglePartMIMEReaderCallbackImpl implements SinglePartMIMEReaderCallback
  {
    final MultiPartMIMEReaderCallback _topLevelCallback;
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData;
    static int partCounter = 0;

    SinglePartMIMEReaderCallbackImpl(final MultiPartMIMEReaderCallback topLevelCallback,
        final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
    {
      _topLevelCallback = topLevelCallback;
      _singlePartMIMEReader = singlePartMIMEReader;
      _headers = singlePartMIMEReader.dataSourceHeaders();
    }

    @Override
    public void onPartDataAvailable(ByteString partData)
    {
      try
      {
        _byteArrayOutputStream.write(partData.copyBytes());
      }
      catch (IOException ioException)
      {
        onStreamError(ioException);
      }
      _singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished()
    {
      partCounter++;
      _finishedData = ByteString.copy(_byteArrayOutputStream.toByteArray());
    }

    //Delegate to the top level for now for these two
    @Override
    public void onAbandoned()
    {
      //This will end up failing the test.
      _topLevelCallback.onAbandoned();
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      //MultiPartMIMEReader will end up calling onStreamError(e) on our top level callback
      //which will fail the test
    }
  }

  private static class MultiPartMIMEReaderCallbackImpl implements MultiPartMIMEReaderCallback
  {
    final Callback<StreamResponse> _r2callback;
    final List<SinglePartMIMEReaderCallbackImpl> _singlePartMIMEReaderCallbacks =
        new ArrayList<SinglePartMIMEReaderCallbackImpl>();

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
    {
      SinglePartMIMEReaderCallbackImpl singlePartMIMEReaderCallback =
          new SinglePartMIMEReaderCallbackImpl(this, singleParMIMEReader);
      singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
      _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
      singleParMIMEReader.requestPartData();
    }

    @Override
    public void onFinished()
    {
      RestResponse response = RestStatus.responseForStatus(RestStatus.OK, "");
      _r2callback.onSuccess(Messages.toStreamResponse(response));
    }

    @Override
    public void onAbandoned()
    {
      RestException restException = new RestException(RestStatus.responseForStatus(406, "Not Acceptable"));
      _r2callback.onError(restException);
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      RestException restException = new RestException(RestStatus.responseForError(400, throwable));
      _r2callback.onError(restException);
    }

    MultiPartMIMEReaderCallbackImpl(final Callback<StreamResponse> r2callback)
    {
      _r2callback = r2callback;
    }
  }

  private static class MimeServerRequestHandler implements StreamRequestHandler
  {
    private MultiPartMIMEReaderCallbackImpl _testMultiPartMIMEReaderCallback;

    MimeServerRequestHandler()
    {
    }

    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext,
        final Callback<StreamResponse> callback)
    {
      try
      {
        MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(request);
        _testMultiPartMIMEReaderCallback = new MultiPartMIMEReaderCallbackImpl(callback);
        reader.registerReaderCallback(_testMultiPartMIMEReaderCallback);
      }
      catch (MultiPartIllegalFormatException illegalMimeFormatException)
      {
        RestException restException = new RestException(RestStatus.responseForError(400, illegalMimeFormatException));
        callback.onError(restException);
      }
    }
  }
}