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
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.linkedin.multipart.utils.MIMETestUtils.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test passing a {@link com.linkedin.multipart.MultiPartMIMEReader} as a data source
 * to the {@link com.linkedin.multipart.MultiPartMIMEWriter}.
 *
 * @author Karim Vidhani
 */
public class TestMIMEChainingReader extends AbstractMIMEUnitTest
{
  //Verifies that a multi part mime reader can be used as a data source to the writer.
  //To make the test easier to write, we simply chain back to the client in the form of simulating a response.
  @Test(dataProvider = "chunkSizes")
  public void testMimeReaderDataSource(final int chunkSize) throws Exception
  {
    final List<MultiPartMIMEDataSource> dataSources =
        generateInputStreamDataSources(chunkSize, _scheduledExecutorService);

    final MultiPartMIMEWriter writer = new MultiPartMIMEWriter.Builder().appendDataSources(dataSources).build();

    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(writer.getEntityStream());
    final String contentTypeHeader = "multipart/mixed; boundary=" + writer.getBoundary();
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);

    //Client side preparation to read the part back on the callback
    //Note the chunks size will carry over since the client is controlling how much data he gets back
    //based on the chunk size he writes.
    final CountDownLatch latch = new CountDownLatch(1);
    ClientMultiPartMIMEReaderReceiver _clientReceiver = new ClientMultiPartMIMEReaderReceiver(latch);
    Callback<StreamResponse> callback = generateSuccessChainCallback(_clientReceiver);

    //Server side start
    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
    ServerMultiPartMIMEChainReaderWriter _serverSender = new ServerMultiPartMIMEChainReaderWriter(callback, reader);
    reader.registerReaderCallback(_serverSender);

    latch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    //Verify client. No need to verify the server.
    List<ClientSinglePartMIMEReaderReceiver> singlePartMIMEReaderCallbacks =
        _clientReceiver._singlePartMIMEReaderCallbacks;

    Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 4);
    Assert.assertEquals(singlePartMIMEReaderCallbacks.get(0)._finishedData, _bodyA.getPartData());
    Assert.assertEquals(singlePartMIMEReaderCallbacks.get(0)._headers, _bodyA.getPartHeaders());
    Assert.assertEquals(singlePartMIMEReaderCallbacks.get(1)._finishedData, _bodyB.getPartData());
    Assert.assertEquals(singlePartMIMEReaderCallbacks.get(1)._headers, _bodyB.getPartHeaders());
    Assert.assertEquals(singlePartMIMEReaderCallbacks.get(2)._finishedData, _bodyC.getPartData());
    Assert.assertEquals(singlePartMIMEReaderCallbacks.get(2)._headers, _bodyC.getPartHeaders());
    Assert.assertEquals(singlePartMIMEReaderCallbacks.get(3)._finishedData, _bodyD.getPartData());
    Assert.assertEquals(singlePartMIMEReaderCallbacks.get(3)._headers, _bodyD.getPartHeaders());
  }

  private Callback<StreamResponse> generateSuccessChainCallback(final ClientMultiPartMIMEReaderReceiver receiver)
  {

    return new Callback<StreamResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        Assert.fail();
      }

      @Override
      public void onSuccess(StreamResponse result)
      {
        MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(result);
        reader.registerReaderCallback(receiver);
      }
    };
  }

  //Client callbacks:
  private static class ClientSinglePartMIMEReaderReceiver implements SinglePartMIMEReaderCallback
  {
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData = null;

    ClientSinglePartMIMEReaderReceiver(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
    {
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
      _finishedData = ByteString.copy(_byteArrayOutputStream.toByteArray());
    }

    @Override
    public void onAbandoned()
    {
      Assert.fail();
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      Assert.fail();
    }
  }

  private static class ClientMultiPartMIMEReaderReceiver implements MultiPartMIMEReaderCallback
  {
    final List<ClientSinglePartMIMEReaderReceiver> _singlePartMIMEReaderCallbacks =
        new ArrayList<ClientSinglePartMIMEReaderReceiver>();
    final CountDownLatch _latch;

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
    {
      ClientSinglePartMIMEReaderReceiver singlePartMIMEReaderCallback =
          new ClientSinglePartMIMEReaderReceiver(singleParMIMEReader);
      singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
      _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
      singleParMIMEReader.requestPartData();
    }

    @Override
    public void onFinished()
    {
      _latch.countDown();
    }

    @Override
    public void onAbandoned()
    {
      Assert.fail();
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      Assert.fail();
    }

    ClientMultiPartMIMEReaderReceiver(final CountDownLatch latch)
    {
      _latch = latch;
    }
  }

  //Server callback. Only the top level needed:
  private static class ServerMultiPartMIMEChainReaderWriter implements MultiPartMIMEReaderCallback
  {
    final Callback<StreamResponse> _callback;
    final MultiPartMIMEReader _reader;

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
    {
      final MultiPartMIMEWriter writer = new MultiPartMIMEWriter.Builder().appendMultiPartDataSource(_reader).build();
      final StreamResponse streamResponse = mock(StreamResponse.class);
      when(streamResponse.getEntityStream()).thenReturn(writer.getEntityStream());
      final String contentTypeHeader = "multipart/mixed; boundary=" + writer.getBoundary();
      when(streamResponse.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);
      _callback.onSuccess(streamResponse);
    }

    @Override
    public void onFinished()
    {
      //Based on the current implementation, this will not be called.
    }

    @Override
    public void onAbandoned()
    {
      Assert.fail();
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      Assert.fail();
    }

    ServerMultiPartMIMEChainReaderWriter(final Callback<StreamResponse> callback, final MultiPartMIMEReader reader)
    {
      _callback = callback;
      _reader = reader;
    }
  }
}