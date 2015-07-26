package com.linkedin.multipart;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.data.ByteString;
import com.linkedin.multipart.reader.exceptions.IllegalMimeFormatException;
import com.linkedin.r2.filter.R2Constants;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by kvidhani on 7/25/15.
 */
public class TestMIMEChainingMultipleSources {


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


    private ScheduledExecutorService _scheduledExecutorService;

    private int _chunkSize;
    MultiPartMIMEDataPartImpl _bodyA;
    MultiPartMIMEDataPartImpl _bodyB;
    MultiPartMIMEDataPartImpl _bodyC;
    MultiPartMIMEDataPartImpl _bodyD;

    MultiPartMIMEDataPartImpl _body1;
    MultiPartMIMEDataPartImpl _body2;
    MultiPartMIMEDataPartImpl _body3;
    MultiPartMIMEDataPartImpl _body4;

    MultiPartMIMEDataPartImpl _localInputStreamBody;

    @BeforeClass
    public void threadPoolSetup() {
        _scheduledExecutorService = Executors.newScheduledThreadPool(10);
    }

    @AfterClass
    public void threadPoolTearDown() {
        _scheduledExecutorService.shutdownNow();
    }

    @BeforeMethod
    public void setup() throws IOException
    {
        _latch = new CountDownLatch(2);

        //First mime body
        final byte[] bodyAbytes = "bodyA".getBytes();
        final Map<String, String> bodyAHeaders = ImmutableMap.of("headerA", "valueA");
        _bodyA = new MultiPartMIMEDataPartImpl(ByteString.copy(bodyAbytes), bodyAHeaders);

        final byte[] bodyBbytes = "bodyB".getBytes();
        final Map<String, String> bodyBHeaders = ImmutableMap.of("headerB", "valueB");
        _bodyB = new MultiPartMIMEDataPartImpl(ByteString.copy(bodyBbytes), bodyBHeaders);

        //body c has no headers
        final byte[] bodyCbytes = "bodyC".getBytes();
        _bodyC = new MultiPartMIMEDataPartImpl(ByteString.copy(bodyCbytes), Collections.<String, String>emptyMap());

        final byte[] bodyDbytes = "bodyD".getBytes();
        final Map<String, String> bodyDHeaders = ImmutableMap.of("headerD", "valueD");
        _bodyD = new MultiPartMIMEDataPartImpl(ByteString.copy(bodyDbytes), bodyDHeaders);

        //Second mime body
        final byte[] body1bytes = "body1".getBytes();
        final Map<String, String> body1Headers = ImmutableMap.of("header1", "value1");
        _body1 = new MultiPartMIMEDataPartImpl(ByteString.copy(body1bytes), body1Headers);

        final byte[] body2bytes = "body2".getBytes();
        final Map<String, String> body2Headers = ImmutableMap.of("header2", "value2");
        _body2 = new MultiPartMIMEDataPartImpl(ByteString.copy(body2bytes), body2Headers);

        //body 3 is completely empty
        _body3 = new MultiPartMIMEDataPartImpl(ByteString.empty(), Collections.<String, String>emptyMap());

        final byte[] body4bytes = "body4".getBytes();
        final Map<String, String> body4Headers = ImmutableMap.of("header4", "value4");
        _body4 = new MultiPartMIMEDataPartImpl(ByteString.copy(body4bytes), body4Headers);

        //Local input stream appended by server 1
        final byte[] localInputStreamBytes = "local input stream".getBytes();
        final Map<String, String> localInputStreamHeaders = ImmutableMap.of("local1", "local2");
        _localInputStreamBody = new MultiPartMIMEDataPartImpl(ByteString.copy(localInputStreamBytes), localInputStreamHeaders);


        _clientFactory = new HttpClientFactory();
        _client = new TransportClientAdapter(_clientFactory.getClient(Collections.<String, String>emptyMap()));
        _server_A_client = new TransportClientAdapter(_clientFactory.getClient(Collections.<String, String>emptyMap()));

        final HttpServerFactory httpServerFactory = new HttpServerFactory();

        final ServerARequestHandler serverARequestHandler = new ServerARequestHandler();
        final TransportDispatcher serverATransportDispatcher = new TransportDispatcherBuilder()
                .addStreamHandler(SERVER_A_URI, serverARequestHandler)
                .build();

        final ServerBRequestHandler serverBRequestHandler = new ServerBRequestHandler();
        final TransportDispatcher serverBTransportDispatcher = new TransportDispatcherBuilder()
                .addStreamHandler(SERVER_B_URI, serverBRequestHandler)
                .build();

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
        {}

        @Override
        public void handleRequest(StreamRequest request, RequestContext requestContext, final Callback<StreamResponse> callback)
        {
            try
            {
                //1. Send a request to server B
                //2. Get a MIME response back
                //3. Tack on a local input stream
                //4. Send the original incoming reader + local input stream + first part from response
                //5. Drain the remaining parts from the response.
                //6. Count down the latch.

                final EntityStream emptyBody = EntityStreams.newEntityStream(new ByteStringWriter(ByteString.empty()));
                final StreamRequest simplePost = new StreamRequestBuilder(Bootstrap.createHttpsURI(PORT_SERVER_B, SERVER_B_URI))
                        .setMethod("POST").build(emptyBody);

                //In this callback, when we get the response from server 2, we will create the compound writer
                Callback<StreamResponse> responseCallback = serverAResponseCallback(request, callback);

                //Send the request to Server A
                _client.streamRequest(simplePost, responseCallback);

            }
            catch (IllegalMimeFormatException illegalMimeFormatException)
            {
                RestException restException = new RestException(RestStatus.responseForError(400, illegalMimeFormatException));
                callback.onError(restException);
            }
        }
    }

    Callback<StreamResponse> serverAResponseCallback(final StreamRequest incomingRequest, final Callback<StreamResponse> incomingRequestCallback) {

        return new Callback<StreamResponse>() {
            @Override
            public void onError(Throwable e) {
                Assert.fail();
            }

            @Override
            public void onSuccess(StreamResponse result) {

                final MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(result);
                ServerAMultiPartCallback serverAMultiPartCallback =
                        new ServerAMultiPartCallback(incomingRequest, incomingRequestCallback);
                reader.registerReaderCallback(serverAMultiPartCallback);

            }
        };
    }



    private class ServerASinglePartCallback implements SinglePartMIMEReaderCallback {

        final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
        final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
        Map<String, String> _headers;
        ByteString _finishedData = null;

        ServerASinglePartCallback(final
                                   MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
            _singlePartMIMEReader = singlePartMIMEReader;
            _headers = singlePartMIMEReader.getHeaders();
        }

        @Override
        public void onPartDataAvailable(ByteString b) {
            try {
                _byteArrayOutputStream.write(b.copyBytes());
            } catch (IOException ioException) {
                onStreamError(ioException);
            }
            _singlePartMIMEReader.requestPartData();
        }

        @Override
        public void onFinished() {
            _finishedData = ByteString.copy(_byteArrayOutputStream.toByteArray());

        }

        //Delegate to the top level for now for these two
        @Override
        public void onAbandoned() {
            Assert.fail();
        }

        @Override
        public void onStreamError(Throwable e) {
            Assert.fail();
        }

    }


    private class ServerAMultiPartCallback implements MultiPartMIMEReaderCallback {

        final Callback<StreamResponse> _incomingRequestCallback;
        final StreamRequest _incomingRequest;
        boolean _firstPartConsumed = false;
        final List<ServerASinglePartCallback> _singlePartMIMEReaderCallbacks =
                new ArrayList<ServerASinglePartCallback>();

        @Override
        public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader) {
            if (!_firstPartConsumed) {
                _firstPartConsumed = true;

                final MultiPartMIMEReader incomingRequestReader = MultiPartMIMEReader.createAndAcquireStream(_incomingRequest);

                final MultiPartMIMEInputStream localInputStream =
                        new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_localInputStreamBody._partData.copyBytes()), _scheduledExecutorService, _bodyC._headers)
                                .withWriteChunkSize(_chunkSize)
                                .build();

                final MultiPartMIMEWriter writer =
                        new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder()
                                .appendDataSource(singleParMIMEReader)
                                .appendDataSource(localInputStream)
                                .appendMultiPartDataSource(incomingRequestReader)
                                .build();

                final StreamResponse streamResponse = new MultiPartMIMEStreamResponseBuilder("mixed", writer, Collections.<String, String>emptyMap()).build();
                _incomingRequestCallback.onSuccess(streamResponse);

            } else {
                ServerASinglePartCallback singlePartMIMEReaderCallback = new ServerASinglePartCallback(singleParMIMEReader);
                singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
                _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
                singleParMIMEReader.requestPartData();
            }
        }

        @Override
        public void onFinished() {
            _latch.countDown();
        }

        @Override
        public void onAbandoned() {
            Assert.fail();
        }

        @Override
        public void onStreamError(Throwable e) {
            Assert.fail();
        }

        ServerAMultiPartCallback(final StreamRequest incomingRequest, final Callback<StreamResponse> callback) {
            _incomingRequest = incomingRequest;
            _incomingRequestCallback = callback;
        }
    }





    private class ServerBRequestHandler implements StreamRequestHandler
    {
        ServerBRequestHandler()
        {}

        @Override
        public void handleRequest(StreamRequest request, RequestContext requestContext, final Callback<StreamResponse> callback)
        {
            try
            {
                final MultiPartMIMEInputStream body1DataSource =
                        new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_body1._partData.copyBytes()), _scheduledExecutorService, _body1._headers)
                                .withWriteChunkSize(_chunkSize)
                                .build();

                final MultiPartMIMEInputStream body2DataSource =
                        new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_body2._partData.copyBytes()), _scheduledExecutorService, _body2._headers)
                                .withWriteChunkSize(_chunkSize)
                                .build();

                final MultiPartMIMEInputStream body3DataSource =
                        new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_body3._partData.copyBytes()), _scheduledExecutorService, _body3._headers)
                                .withWriteChunkSize(_chunkSize)
                                .build();

                final MultiPartMIMEInputStream body4DataSource =
                        new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_body4._partData.copyBytes()), _scheduledExecutorService, _body4._headers)
                                .withWriteChunkSize(_chunkSize)
                                .build();

                final List<MultiPartMIMEDataSource> dataSources = new ArrayList<MultiPartMIMEDataSource>();
                dataSources.add(body1DataSource);
                dataSources.add(body2DataSource);
                dataSources.add(body3DataSource);
                dataSources.add(body4DataSource);

                final MultiPartMIMEWriter writer =
                        new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder()
                                .appendDataSources(dataSources)
                                .build();

                final StreamResponse streamResponse = new MultiPartMIMEStreamResponseBuilder("mixed", writer, Collections.<String, String>emptyMap()).build();
                callback.onSuccess(streamResponse);

            }
            catch (IllegalMimeFormatException illegalMimeFormatException)
            {
                RestException restException = new RestException(RestStatus.responseForError(400, illegalMimeFormatException));
                callback.onError(restException);
            }
        }
    }









    //main thread
    //server 1
    //server 2

    //main thread sends mime
    //server 1 sends a request to server 2
    //server 2 sends back a mime response
    //server 1 takes a local input stream + the incoming original mime request plus the first part from the incoming mime response
    //main thread then gets all of this and stores it
    //server 1 then drains and stores the rest of the parts from server 2's response.

    //5. Client sends a MM request to a server. Server sends a request to another server and gets back a MM response.
    //We now have two readers. It consumes the first part from the second one and sends the original MM, a local input stream
    //and and the 2nd part from the second MM to a third server. The second server then complete drains the remaining bits from the 2nd MM.
    //Assert everyone got what they wanted.

    @DataProvider(name = "chunkSizes")
    public Object[][] chunkSizes() throws Exception {
        return new Object[][]{
                {1},
                {R2Constants.DEFAULT_DATA_CHUNK_SIZE}
        };
    }

    @Test(dataProvider = "chunkSizes")
    public void testSinglePartDataSource(final int chunkSize) throws Exception {

        _chunkSize = chunkSize;

        final MultiPartMIMEInputStream bodyADataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyA._partData.copyBytes()), _scheduledExecutorService, _bodyA._headers)
                        .withWriteChunkSize(chunkSize)
                        .build();

        final MultiPartMIMEInputStream bodyBDataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyB._partData.copyBytes()), _scheduledExecutorService, _bodyB._headers)
                        .withWriteChunkSize(chunkSize)
                        .build();

        final MultiPartMIMEInputStream bodyCDataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyC._partData.copyBytes()), _scheduledExecutorService, _bodyC._headers)
                        .withWriteChunkSize(chunkSize)
                        .build();

        final MultiPartMIMEInputStream bodyDDataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyD._partData.copyBytes()), _scheduledExecutorService, _bodyD._headers)
                        .withWriteChunkSize(chunkSize)
                        .build();

        final List<MultiPartMIMEDataSource> dataSources = new ArrayList<MultiPartMIMEDataSource>();
        dataSources.add(bodyADataSource);
        dataSources.add(bodyBDataSource);
        dataSources.add(bodyCDataSource);
        dataSources.add(bodyDDataSource);

        final MultiPartMIMEWriter writer =
                new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder()
                        .appendDataSources(dataSources)
                        .build();

        final StreamRequest multiPartMIMEStreamRequest = new MultiPartMIMEStreamRequestBuilder(Bootstrap.createHttpsURI(PORT_SERVER_A, SERVER_A_URI),
                "mixed", writer, Collections.<String, String>emptyMap()).build();

        ClientMultiPartMIMEReaderEchoReceiver _clientReceiver =
                new ClientMultiPartMIMEReaderEchoReceiver();
        Callback<StreamResponse> callback = expectSuccessChainCallback(_clientReceiver);

        //Send the request to Server A
        _client.streamRequest(multiPartMIMEStreamRequest, callback);


        _latch.await(60000, TimeUnit.MILLISECONDS);

        //Verify client
        Assert.assertEquals(_clientReceiver._singlePartMIMEReaderCallbacks.size(), 6);
        //Assert.assertEquals(_clientReceiver._singlePartMIMEReaderCallbacks.get(0)._finishedData, _bodyA.getPartData());
        //Assert.assertEquals(_clientReceiver._singlePartMIMEReaderCallbacks.get(0)._headers, _bodyA.getPartHeaders());

        //Verify server
        //List<ServerSinglePartMIMEReader> singlePartMIMEReaderCallbacks =
        //        _serverSender._singlePartMIMEReaderCallbacks;
        /*
        Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 3);
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(0)._finishedData, _bodyB.getPartData());
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(0)._headers, _bodyB.getPartHeaders());
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(1)._finishedData, _bodyC.getPartData());
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(1)._headers, _bodyC.getPartHeaders());
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(2)._finishedData, _bodyD.getPartData());
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(2)._headers, _bodyD.getPartHeaders());
        */
    }

    static Callback<StreamResponse> expectSuccessChainCallback(final ClientMultiPartMIMEReaderEchoReceiver receiver) {

        return new Callback<StreamResponse>() {
            @Override
            public void onError(Throwable e) {
                Assert.fail();
            }

            @Override
            public void onSuccess(StreamResponse result) {
                final MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(result);
                reader.registerReaderCallback(receiver);
            }
        };
    }


    //Client callbacks:
    private  class ClientSinglePartMIMEReaderEchoReceiver implements SinglePartMIMEReaderCallback {

        final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
        final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
        Map<String, String> _headers;
        ByteString _finishedData = null;

        ClientSinglePartMIMEReaderEchoReceiver(final
                                               MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader) {
            _singlePartMIMEReader = singlePartMIMEReader;
            _headers = singlePartMIMEReader.getHeaders();
        }

        @Override
        public void onPartDataAvailable(ByteString b) {
            try {
                _byteArrayOutputStream.write(b.copyBytes());
            } catch (IOException ioException) {
                onStreamError(ioException);
            }
            _singlePartMIMEReader.requestPartData();
        }

        @Override
        public void onFinished() {
            _finishedData = ByteString.copy(_byteArrayOutputStream.toByteArray());
        }

        @Override
        public void onAbandoned() {
            Assert.fail();
        }

        @Override
        public void onStreamError(Throwable e) {
            Assert.fail();
        }

    }


    private  class ClientMultiPartMIMEReaderEchoReceiver implements MultiPartMIMEReaderCallback {

        final List<ClientSinglePartMIMEReaderEchoReceiver> _singlePartMIMEReaderCallbacks =
                new ArrayList<ClientSinglePartMIMEReaderEchoReceiver>();

        @Override
        public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader) {
            ClientSinglePartMIMEReaderEchoReceiver singlePartMIMEReaderCallback = new ClientSinglePartMIMEReaderEchoReceiver(singleParMIMEReader);
            singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
            _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
            singleParMIMEReader.requestPartData();
        }

        @Override
        public void onFinished() {
            //We don't have to do anything here.
            _latch.countDown();
        }

        @Override
        public void onAbandoned() {
            Assert.fail();
        }

        @Override
        public void onStreamError(Throwable e) {
            Assert.fail();
        }

        ClientMultiPartMIMEReaderEchoReceiver() {
        }
    }




}
