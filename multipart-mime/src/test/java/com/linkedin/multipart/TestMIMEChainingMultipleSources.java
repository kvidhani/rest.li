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
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.data.ByteString;
import com.linkedin.multipart.exceptions.IllegalMultiPartMIMEFormatException;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.*;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.sample.Bootstrap;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.StreamRequestHandler;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.server.HttpServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;

import org.testng.Assert;
import org.testng.annotations.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.linkedin.multipart.utils.MIMETestUtils.*;


/**
 * Represents a test where we write an envelope containing a {@link com.linkedin.multipart.MultiPartMIMEReader.SinglePartMIMEReader},
 * a {@link com.linkedin.multipart.MultiPartMIMEReader} and a {@link com.linkedin.multipart.MultiPartMIMEInputStream}.
 *
 * @author Karim Vidhani
 */
public class TestMIMEChainingMultipleSources extends AbstractMIMEUnitTest
{
  private static final int PORT_SERVER_A = 8450;
  private static final int PORT_SERVER_B = 8451;
  private static final URI SERVER_A_URI = URI.create("/serverA");
  private static final URI SERVER_B_URI = URI.create("/serverB");
  private TransportClientFactory _clientFactory;
  private HttpServer _server_A;
  private HttpServer _server_B;
  private Client _client;
  private Client _server_A_client;
  private CountDownLatch _latch;
  private ServerAMultiPartCallback _serverAMultiPartCallback;
  private int _chunkSize;

  @BeforeMethod
  public void setup() throws IOException
  {
    _latch = new CountDownLatch(2);
    _clientFactory = new HttpClientFactory();
    _client = new TransportClientAdapter(_clientFactory.getClient(Collections.<String, String>emptyMap()));
    _server_A_client = new TransportClientAdapter(_clientFactory.getClient(Collections.<String, String>emptyMap()));

    final HttpServerFactory httpServerFactory = new HttpServerFactory();

    final ServerARequestHandler serverARequestHandler = new ServerARequestHandler();
    final TransportDispatcher serverATransportDispatcher =
        new TransportDispatcherBuilder().addStreamHandler(SERVER_A_URI, serverARequestHandler).build();

    final ServerBRequestHandler serverBRequestHandler = new ServerBRequestHandler();
    final TransportDispatcher serverBTransportDispatcher =
        new TransportDispatcherBuilder().addStreamHandler(SERVER_B_URI, serverBRequestHandler).build();

    _server_A = httpServerFactory.createServer(PORT_SERVER_A, serverATransportDispatcher);
    _server_B = httpServerFactory.createServer(PORT_SERVER_B, serverBTransportDispatcher);
    _server_A.start();
    _server_B.start();
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
    final FutureCallback<None> clientShutdownCallback = new FutureCallback<None>();
    _client.shutdown(clientShutdownCallback);
    clientShutdownCallback.get();

    final FutureCallback<None> server1ClientShutdownCallback = new FutureCallback<None>();
    _server_A_client.shutdown(server1ClientShutdownCallback);
    server1ClientShutdownCallback.get();

    final FutureCallback<None> factoryShutdownCallback = new FutureCallback<None>();
    _clientFactory.shutdown(factoryShutdownCallback);
    factoryShutdownCallback.get();

    _server_A.stop();
    _server_A.waitForStop();
    _server_B.stop();
    _server_B.waitForStop();
  }

  private class ServerARequestHandler implements StreamRequestHandler
  {
    ServerARequestHandler()
    {
    }

    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext,
        final Callback<StreamResponse> callback)
    {
      try
      {
        //1. Send a request to server B.
        //2. Get a MIME response back.
        //3. Tack on a local input stream (_body5).
        //4. Send the original incoming reader + local input stream + first part from the incoming response.
        //5. Drain the remaining parts from the response.
        //6. Count down the latch.

        final EntityStream emptyBody = EntityStreams.newEntityStream(new ByteStringWriter(ByteString.empty()));
        final StreamRequest simplePost =
            new StreamRequestBuilder(Bootstrap.createHttpsURI(PORT_SERVER_B, SERVER_B_URI)).setMethod("POST")
                .build(emptyBody);

        //In this callback, when we get the response from server 2, we will create the compound writer
        Callback<StreamResponse> responseCallback = generateServerAResponseCallback(request, callback);

        //Send the request to Server A
        _client.streamRequest(simplePost, responseCallback);
      }
      catch (IllegalMultiPartMIMEFormatException illegalMimeFormatException)
      {
        RestException restException = new RestException(RestStatus.responseForError(400, illegalMimeFormatException));
        callback.onError(restException);
      }
    }
  }

  private Callback<StreamResponse> generateServerAResponseCallback(final StreamRequest incomingRequest,
      final Callback<StreamResponse> incomingRequestCallback)
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
        final MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(result);
        _serverAMultiPartCallback = new ServerAMultiPartCallback(incomingRequest, incomingRequestCallback);
        reader.registerReaderCallback(_serverAMultiPartCallback);
      }
    };
  }

  private class ServerASinglePartCallback implements SinglePartMIMEReaderCallback
  {
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData = null;

    ServerASinglePartCallback(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
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

  private class ServerAMultiPartCallback implements MultiPartMIMEReaderCallback
  {
    final Callback<StreamResponse> _incomingRequestCallback;
    final StreamRequest _incomingRequest;
    boolean _firstPartConsumed = false;
    final List<ServerASinglePartCallback> _singlePartMIMEReaderCallbacks = new ArrayList<ServerASinglePartCallback>();

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
    {
      if (!_firstPartConsumed)
      {
        _firstPartConsumed = true;

        final MultiPartMIMEReader incomingRequestReader = MultiPartMIMEReader.createAndAcquireStream(_incomingRequest);

        final MultiPartMIMEInputStream localInputStream =
            new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_body5.getPartData().copyBytes()),
                _scheduledExecutorService, _body5.getPartHeaders()).withWriteChunkSize(_chunkSize).build();

        final MultiPartMIMEWriter writer =
            new MultiPartMIMEWriter.Builder().appendDataSource(singleParMIMEReader).appendDataSource(localInputStream)
                .appendMultiPartDataSource(incomingRequestReader).build();

        final StreamResponse streamResponse =
            MultiPartMIMEStreamResponseBuilder.generateMultiPartMIMEStreamResponse("mixed", writer, Collections.<String, String>emptyMap());
        _incomingRequestCallback.onSuccess(streamResponse);
      }
      else
      {
        ServerASinglePartCallback singlePartMIMEReaderCallback = new ServerASinglePartCallback(singleParMIMEReader);
        singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
        _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
        singleParMIMEReader.requestPartData();
      }
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

    ServerAMultiPartCallback(final StreamRequest incomingRequest, final Callback<StreamResponse> callback)
    {
      _incomingRequest = incomingRequest;
      _incomingRequestCallback = callback;
    }
  }

  private class ServerBRequestHandler implements StreamRequestHandler
  {
    ServerBRequestHandler()
    {
    }

    @Override
    public void handleRequest(StreamRequest request, RequestContext requestContext,
        final Callback<StreamResponse> callback)
    {
      try
      {
        final MultiPartMIMEInputStream body1DataSource =
            new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_body1.getPartData().copyBytes()),
                _scheduledExecutorService, _body1.getPartHeaders()).withWriteChunkSize(_chunkSize).build();

        final MultiPartMIMEInputStream body2DataSource =
            new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_body2.getPartData().copyBytes()),
                _scheduledExecutorService, _body2.getPartHeaders()).withWriteChunkSize(_chunkSize).build();

        final MultiPartMIMEInputStream body3DataSource =
            new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_body3.getPartData().copyBytes()),
                _scheduledExecutorService, _body3.getPartHeaders()).withWriteChunkSize(_chunkSize).build();

        final MultiPartMIMEInputStream body4DataSource =
            new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_body4.getPartData().copyBytes()),
                _scheduledExecutorService, _body4.getPartHeaders()).withWriteChunkSize(_chunkSize).build();

        final List<MultiPartMIMEDataSource> dataSources = new ArrayList<MultiPartMIMEDataSource>();
        dataSources.add(body1DataSource);
        dataSources.add(body2DataSource);
        dataSources.add(body3DataSource);
        dataSources.add(body4DataSource);

        final MultiPartMIMEWriter writer = new MultiPartMIMEWriter.Builder().appendDataSources(dataSources).build();

        final StreamResponse streamResponse =
            MultiPartMIMEStreamResponseBuilder.generateMultiPartMIMEStreamResponse("mixed", writer, Collections.<String, String>emptyMap());
        callback.onSuccess(streamResponse);
      }
      catch (IllegalMultiPartMIMEFormatException illegalMimeFormatException)
      {
        RestException restException = new RestException(RestStatus.responseForError(400, illegalMimeFormatException));
        callback.onError(restException);
      }
    }
  }

  //Test breakdown:
  //1. Main thread sends mime request
  //2. Server 1 sends a simple POST request to server 2
  //3. Server 2 sends back a mime response to server 1
  //4. Server 1 takes the original incoming request from the main thread + a local input
  //stream + the first part from the incoming mime response from server 2.
  //5. Main thread then gets all of this and stores it.
  //6. Server 1 then drains and stores the rest of the parts from server 2's response.
  @Test(dataProvider = "chunkSizes")
  public void testSinglePartDataSource(final int chunkSize) throws Exception
  {
    _chunkSize = chunkSize;

    final List<MultiPartMIMEDataSource> dataSources =
        generateInputStreamDataSources(chunkSize, _scheduledExecutorService);

    final MultiPartMIMEWriter writer = new MultiPartMIMEWriter.Builder().appendDataSources(dataSources).build();

    final StreamRequest streamRequest =
        MultiPartMIMEStreamRequestBuilder.generateMultiPartMIMEStreamRequest(Bootstrap.createHttpsURI(PORT_SERVER_A, SERVER_A_URI),
            "mixed", writer, Collections.<String, String>emptyMap());

    ClientMultiPartReceiver clientReceiver = new ClientMultiPartReceiver();
    Callback<StreamResponse> callback = generateSuccessChainCallback(clientReceiver);

    //Send the request to Server A
    _client.streamRequest(streamRequest, callback);

    _latch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    //Verify client
    List<ClientSinglePartReceiver> clientSinglePartCallbacks = clientReceiver._singlePartMIMEReaderCallbacks;
    Assert.assertEquals(clientReceiver._singlePartMIMEReaderCallbacks.size(), 6);
    Assert.assertEquals(clientSinglePartCallbacks.get(0)._finishedData, _body1.getPartData());
    Assert.assertEquals(clientSinglePartCallbacks.get(0)._headers, _body1.getPartHeaders());
    Assert.assertEquals(clientSinglePartCallbacks.get(1)._finishedData, _body5.getPartData());
    Assert.assertEquals(clientSinglePartCallbacks.get(1)._headers, _body5.getPartHeaders());
    Assert.assertEquals(clientSinglePartCallbacks.get(2)._finishedData, _bodyA.getPartData());
    Assert.assertEquals(clientSinglePartCallbacks.get(2)._headers, _bodyA.getPartHeaders());
    Assert.assertEquals(clientSinglePartCallbacks.get(3)._finishedData, _bodyB.getPartData());
    Assert.assertEquals(clientSinglePartCallbacks.get(3)._headers, _bodyB.getPartHeaders());
    Assert.assertEquals(clientSinglePartCallbacks.get(4)._finishedData, _bodyC.getPartData());
    Assert.assertEquals(clientSinglePartCallbacks.get(4)._headers, _bodyC.getPartHeaders());
    Assert.assertEquals(clientSinglePartCallbacks.get(5)._finishedData, _bodyD.getPartData());
    Assert.assertEquals(clientSinglePartCallbacks.get(5)._headers, _bodyD.getPartHeaders());

    //Verify Server A
    List<ServerASinglePartCallback> serverASinglePartCallbacks =
        _serverAMultiPartCallback._singlePartMIMEReaderCallbacks;
    Assert.assertEquals(serverASinglePartCallbacks.size(), 3);
    Assert.assertEquals(serverASinglePartCallbacks.get(0)._finishedData, _body2.getPartData());
    Assert.assertEquals(serverASinglePartCallbacks.get(0)._headers, _body2.getPartHeaders());
    Assert.assertEquals(serverASinglePartCallbacks.get(1)._finishedData, _body3.getPartData());
    Assert.assertEquals(serverASinglePartCallbacks.get(1)._headers, _body3.getPartHeaders());
    Assert.assertEquals(serverASinglePartCallbacks.get(2)._finishedData, _body4.getPartData());
    Assert.assertEquals(serverASinglePartCallbacks.get(2)._headers, _body4.getPartHeaders());
  }

  private Callback<StreamResponse> generateSuccessChainCallback(final ClientMultiPartReceiver receiver)
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
        final MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(result);
        reader.registerReaderCallback(receiver);
      }
    };
  }

  //Client callbacks:
  private class ClientSinglePartReceiver implements SinglePartMIMEReaderCallback
  {
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData = null;

    ClientSinglePartReceiver(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
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

  private class ClientMultiPartReceiver implements MultiPartMIMEReaderCallback
  {
    final List<ClientSinglePartReceiver> _singlePartMIMEReaderCallbacks = new ArrayList<ClientSinglePartReceiver>();

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
    {
      ClientSinglePartReceiver singlePartMIMEReaderCallback = new ClientSinglePartReceiver(singleParMIMEReader);
      singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
      _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
      singleParMIMEReader.requestPartData();
    }

    @Override
    public void onFinished()
    {
      //We don't have to do anything here.
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

    ClientMultiPartReceiver()
    {
    }
  }
}