package com.linkedin.multipart;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
public class TestMIMEChainingAlternate {

    private static ScheduledExecutorService scheduledExecutorService;

    MultiPartMIMEDataPartImpl _bodyA;
    MultiPartMIMEDataPartImpl _bodyB;
    MultiPartMIMEDataPartImpl _bodyC;
    MultiPartMIMEDataPartImpl _bodyD;

    @BeforeTest
    public void dataSourceSetup() {

        scheduledExecutorService = Executors.newScheduledThreadPool(10);

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

    }



    //This test has the server alternate between consuming a part and sending a part as a data source
    //to a writer.
    //Since we have four parts, the server will consume the 2nd and 4th and send out the 1st and 3rd.
    //To make the test easier we will have two callbacks to send to the server to indicate
    //the presence of each data source.
    //This violates the typical client/server http pattern, but accomplishes the purpose of this test
    //and it makes it easier to write.

    @DataProvider(name = "chunkSizes")
    public Object[][] chunkSizes() throws Exception {
        return new Object[][]{
                {1},
                {R2Constants.DEFAULT_DATA_CHUNK_SIZE}
        };
    }

    @Test(dataProvider = "chunkSizes")
    public void testAlternateSinglePartDataSource(final int chunkSize) throws Exception {

        final MultiPartMIMEInputStream bodyADataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyA.getPartData().copyBytes()), scheduledExecutorService, _bodyA.getPartHeaders())
                        .withWriteChunkSize(chunkSize)
                        .build();

        final MultiPartMIMEInputStream bodyBDataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyB.getPartData().copyBytes()), scheduledExecutorService, _bodyB.getPartHeaders())
                        .withWriteChunkSize(chunkSize)
                        .build();

        final MultiPartMIMEInputStream bodyCDataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyC.getPartData().copyBytes()), scheduledExecutorService, _bodyC.getPartHeaders())
                        .withWriteChunkSize(chunkSize)
                        .build();

        final MultiPartMIMEInputStream bodyDDataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyD.getPartData().copyBytes()), scheduledExecutorService, _bodyD.getPartHeaders())
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

        final StreamRequest streamRequest = mock(StreamRequest.class);
        when(streamRequest.getEntityStream()).thenReturn(writer.getEntityStream());
        final String contentTypeHeader = "multipart/mixed; boundary=" + writer.getBoundary();
        when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);

        //Client side preparation to read the part back on the callback.
        //We have tow callbacks here since we will get two responses.
        //Note the chunks size will carry over since the client is controlling how much data he gets back
        //based on the chunk size he writes.
        ClientMultiPartMIMEReaderEchoReceiver _clientReceiverA =
                new ClientMultiPartMIMEReaderEchoReceiver();
        ClientMultiPartMIMEReaderEchoReceiver _clientReceiverB =
                new ClientMultiPartMIMEReaderEchoReceiver();
        Callback<StreamResponse> callbackA = expectSuccessChainCallback(_clientReceiverA);
        Callback<StreamResponse> callbackB = expectSuccessChainCallback(_clientReceiverB);

        //Server side start
        MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
        final CountDownLatch latch = new CountDownLatch(1);
        ServerMultiPartMIMEReaderSinglePartEcho _serverSender =
                new ServerMultiPartMIMEReaderSinglePartEcho(latch, callbackA, callbackB);
        reader.registerReaderCallback(_serverSender);

        latch.await(60000, TimeUnit.MILLISECONDS);

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

    static Callback<StreamResponse> expectSuccessChainCallback(final ClientMultiPartMIMEReaderEchoReceiver receiver) {

        return new Callback<StreamResponse>() {
            @Override
            public void onError(Throwable e) {
                Assert.fail();
            }

            @Override
            public void onSuccess(StreamResponse result) {
                MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(result);
                reader.registerReaderCallback(receiver);
            }
        };
    }


    //Client callbacks:
    private static class ClientSinglePartMIMEReaderEchoReceiver implements SinglePartMIMEReaderCallback {

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


    private static class ClientMultiPartMIMEReaderEchoReceiver implements MultiPartMIMEReaderCallback {

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


    //Server callbacks:
    private static class ServerSinglePartMIMEReader implements SinglePartMIMEReaderCallback {

        final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
        final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
        Map<String, String> _headers;
        ByteString _finishedData = null;

        ServerSinglePartMIMEReader(final
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


    private static class ServerMultiPartMIMEReaderSinglePartEcho implements MultiPartMIMEReaderCallback {

        final CountDownLatch _latch;
        final Callback<StreamResponse> _callbackA;
        final Callback<StreamResponse> _callbackB;
        final List<ServerSinglePartMIMEReader> _singlePartMIMEReaderCallbacks =
                new ArrayList<ServerSinglePartMIMEReader>();
        int _currentPart = 0;

        @Override
        public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader) {
            _currentPart ++;
            if (_currentPart == 1) {
                final MultiPartMIMEWriter writer =
                        new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder().appendDataSource(singleParMIMEReader).build();

                final StreamResponse streamResponse = mock(StreamResponse.class);
                when(streamResponse.getEntityStream()).thenReturn(writer.getEntityStream());
                final String contentTypeHeader = "multipart/mixed; boundary=" + writer.getBoundary();
                when(streamResponse.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);
                _callbackA.onSuccess(streamResponse);
            } else if (_currentPart == 3) {
                final MultiPartMIMEWriter writer =
                        new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder().appendDataSource(singleParMIMEReader).build();

                final StreamResponse streamResponse = mock(StreamResponse.class);
                when(streamResponse.getEntityStream()).thenReturn(writer.getEntityStream());
                final String contentTypeHeader = "multipart/mixed; boundary=" + writer.getBoundary();
                when(streamResponse.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);
                _callbackB.onSuccess(streamResponse);

            } else {
                //Consume 2 and 4
                ServerSinglePartMIMEReader singlePartMIMEReaderCallback = new ServerSinglePartMIMEReader(singleParMIMEReader);
                singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
                _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
                singleParMIMEReader.requestPartData();
            }
        }

        @Override
        public void onFinished() {
            //Now we can assert everywhere
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

        ServerMultiPartMIMEReaderSinglePartEcho(final CountDownLatch latch,
                                                final Callback<StreamResponse> callbackA,
                                                final Callback<StreamResponse> callbackB) {
            _latch = latch;
            _callbackA = callbackA;
            _callbackB = callbackB;
        }
    }

}
