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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static com.linkedin.multipart.utils.MIMETestUtils.*;


/**
 * Represents a test where we alternate between chaining and consuming a
 * {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader}.
 *
 * @author Karim Vidhani
 */
public class TestMIMEChainingAlternate extends AbstractMIMEUnitTest
{
  //This test has the server alternate between consuming a part and sending a part as a data source
  //to a writer.
  //Since we have four parts, the server will consume the 2nd and 4th and send out the 1st and 3rd.
  //To make the test easier we will have two callbacks to send to the server to indicate
  //the presence of each data source.
  //This violates the typical client/server http pattern, but accomplishes the purpose of this test
  //and it makes it easier to write.
  @Test(dataProvider = "chunkSizes")
  public void testAlternateSinglePartDataSource(final int chunkSize) throws Exception
  {
    final List<MultiPartMIMEDataSource> dataSources =
        generateInputStreamDataSources(chunkSize, _scheduledExecutorService);

    final MultiPartMIMEWriter writer = new MultiPartMIMEWriter.Builder().appendDataSources(dataSources).build();

    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(writer.getEntityStream());
    final String contentTypeHeader = "multipart/mixed; boundary=" + writer.getBoundary();
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);

    //Client side preparation to read the part back on the callback.
    //We have two callbacks here since we will get two responses.
    //Note the chunks size will carry over since the client is controlling how much data he gets back
    //based on the chunk size he writes.
    ClientMultiPartAlternateReceiver _clientReceiverA = new ClientMultiPartAlternateReceiver();
    ClientMultiPartAlternateReceiver _clientReceiverB = new ClientMultiPartAlternateReceiver();
    Callback<StreamResponse> callbackA = generateSuccessChainCallback(_clientReceiverA);
    Callback<StreamResponse> callbackB = generateSuccessChainCallback(_clientReceiverB);

    //Server side start
    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
    final CountDownLatch latch = new CountDownLatch(1);
    ServerMultiPartMIMEAlternator _serverSender = new ServerMultiPartMIMEAlternator(latch, callbackA, callbackB);
    reader.registerReaderCallback(_serverSender);

    latch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    //Verify client
    Assert.assertEquals(_clientReceiverA._singlePartMIMEReaderCallbacks.size(), 1);
    Assert.assertEquals(_clientReceiverA._singlePartMIMEReaderCallbacks.get(0)._finishedData, _bodyA.getPartData());
    Assert.assertEquals(_clientReceiverA._singlePartMIMEReaderCallbacks.get(0)._headers, _bodyA.getPartHeaders());

    Assert.assertEquals(_clientReceiverB._singlePartMIMEReaderCallbacks.size(), 1);
    Assert.assertEquals(_clientReceiverB._singlePartMIMEReaderCallbacks.get(0)._finishedData, _bodyC.getPartData());
    Assert.assertEquals(_clientReceiverB._singlePartMIMEReaderCallbacks.get(0)._headers, _bodyC.getPartHeaders());

    //Verify server
    Assert.assertEquals(_serverSender._singlePartMIMEReaderCallbacks.size(), 2);
    Assert.assertEquals(_serverSender._singlePartMIMEReaderCallbacks.get(0)._finishedData, _bodyB.getPartData());
    Assert.assertEquals(_serverSender._singlePartMIMEReaderCallbacks.get(0)._headers, _bodyB.getPartHeaders());
    Assert.assertEquals(_serverSender._singlePartMIMEReaderCallbacks.get(1)._finishedData, _bodyD.getPartData());
    Assert.assertEquals(_serverSender._singlePartMIMEReaderCallbacks.get(1)._headers, _bodyD.getPartHeaders());
  }

  private Callback<StreamResponse> generateSuccessChainCallback(final ClientMultiPartAlternateReceiver receiver)
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
  private static class ClientSinglePartAlternateReceiver implements SinglePartMIMEReaderCallback
  {
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData = null;

    ClientSinglePartAlternateReceiver(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
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

  private static class ClientMultiPartAlternateReceiver implements MultiPartMIMEReaderCallback
  {
    final List<ClientSinglePartAlternateReceiver> _singlePartMIMEReaderCallbacks = new ArrayList<ClientSinglePartAlternateReceiver>();

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
    {
      ClientSinglePartAlternateReceiver singlePartMIMEReaderCallback =
          new ClientSinglePartAlternateReceiver(singleParMIMEReader);
      singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
      _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
      singleParMIMEReader.requestPartData();
    }

    @Override
    public void onFinished()
    {
      //We don't have to do anything here.
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

    ClientMultiPartAlternateReceiver()
    {
    }
  }

  //Server callbacks:
  private static class ServerSinglePartMIMEReader implements SinglePartMIMEReaderCallback
  {
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData = null;

    ServerSinglePartMIMEReader(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
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

    //Delegate to the top level for now for these two
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

  private static class ServerMultiPartMIMEAlternator implements MultiPartMIMEReaderCallback
  {
    final CountDownLatch _latch;
    final Callback<StreamResponse> _callbackA;
    final Callback<StreamResponse> _callbackB;
    final List<ServerSinglePartMIMEReader> _singlePartMIMEReaderCallbacks = new ArrayList<ServerSinglePartMIMEReader>();
    int _currentPart = 0;

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
    {
      _currentPart++;
      if (_currentPart == 1)
      {
        final MultiPartMIMEWriter writer =
            new MultiPartMIMEWriter.Builder().appendDataSource(singleParMIMEReader).build();

        final StreamResponse streamResponse = mock(StreamResponse.class);
        when(streamResponse.getEntityStream()).thenReturn(writer.getEntityStream());
        final String contentTypeHeader = "multipart/mixed; boundary=" + writer.getBoundary();
        when(streamResponse.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);
        _callbackA.onSuccess(streamResponse);
      }
      else if (_currentPart == 3)
      {
        final MultiPartMIMEWriter writer =
            new MultiPartMIMEWriter.Builder().appendDataSource(singleParMIMEReader).build();

        final StreamResponse streamResponse = mock(StreamResponse.class);
        when(streamResponse.getEntityStream()).thenReturn(writer.getEntityStream());
        final String contentTypeHeader = "multipart/mixed; boundary=" + writer.getBoundary();
        when(streamResponse.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);
        _callbackB.onSuccess(streamResponse);
      }
      else
      {
        //Consume 2 and 4
        ServerSinglePartMIMEReader singlePartMIMEReaderCallback = new ServerSinglePartMIMEReader(singleParMIMEReader);
        singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
        _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
        singleParMIMEReader.requestPartData();
      }
    }

    @Override
    public void onFinished()
    {
      //Now we can assert everywhere
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

    ServerMultiPartMIMEAlternator(final CountDownLatch latch, final Callback<StreamResponse> callbackA,
        final Callback<StreamResponse> callbackB)
    {
      _latch = latch;
      _callbackA = callbackA;
      _callbackB = callbackB;
    }
  }
}