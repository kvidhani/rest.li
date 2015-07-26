package com.linkedin.multipart;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.rest.*;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by kvidhani on 7/22/15.
 */

public class TestMIMEChainingSinglePart {


    //todo - do this!
    //todo change the name fo this file
    //todo test the case where when we chain, and register a new top level multipart mime reader callback, we throw
    //from:
    //multipart mime reader callback:
    //onNewPart both in the main logic and also in callback registration


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





    //Verifies that a single part mime reader can be used as a data source to the writer.
//To make the test easier to write, we simply chain back to the client in the form of simulating a response.

    //This test creates a multipart mime request. On the server side, upon invocation to onNewPart(), the server
    //creates a writer to send back that first part. When the client finishes receiving this,
    //the server picks up where they left off and consumes the rest.
    @DataProvider(name = "chunkSizes")
    public Object[][] chunkSizes() throws Exception {
        return new Object[][]{
                {1},
                {R2Constants.DEFAULT_DATA_CHUNK_SIZE}
        };
    }

    @Test(dataProvider = "chunkSizes")
    public void testSinglePartDataSource(final int chunkSize) throws Exception {

        final MultiPartMIMEInputStream bodyADataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyA._partData.copyBytes()), scheduledExecutorService, _bodyA._headers)
                        .withWriteChunkSize(chunkSize)
                        .build();

        final MultiPartMIMEInputStream bodyBDataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyB._partData.copyBytes()), scheduledExecutorService, _bodyB._headers)
                        .withWriteChunkSize(chunkSize)
                        .build();

        final MultiPartMIMEInputStream bodyCDataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyC._partData.copyBytes()), scheduledExecutorService, _bodyC._headers)
                        .withWriteChunkSize(chunkSize)
                        .build();

        final MultiPartMIMEInputStream bodyDDataSource =
                new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(_bodyD._partData.copyBytes()), scheduledExecutorService, _bodyD._headers)
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

        //Client side preparation to read the part back on the callback
        //Note the chunks size will carry over since the client is controlling how much data he gets back
        //based on the chunk size he writes.
        ClientMultiPartMIMEReaderEchoReceiver _clientReceiver =
                new ClientMultiPartMIMEReaderEchoReceiver();
        Callback<StreamResponse> callback = expectSuccessChainCallback(_clientReceiver);

        //Server side start
        MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
        final CountDownLatch latch = new CountDownLatch(1);
        ServerMultiPartMIMEReaderSinglePartEcho _serverSender =
                new ServerMultiPartMIMEReaderSinglePartEcho(latch, callback);
        reader.registerReaderCallback(_serverSender);

        latch.await(60000, TimeUnit.MILLISECONDS);

        //Verify client
        Assert.assertEquals(_clientReceiver._singlePartMIMEReaderCallbacks.size(), 1);
        Assert.assertEquals(_clientReceiver._singlePartMIMEReaderCallbacks.get(0)._finishedData, _bodyA.getPartData());
        Assert.assertEquals(_clientReceiver._singlePartMIMEReaderCallbacks.get(0)._headers, _bodyA.getPartHeaders());

        //Verify server
        List<ServerSinglePartMIMEReader> singlePartMIMEReaderCallbacks =
                _serverSender._singlePartMIMEReaderCallbacks;
        Assert.assertEquals(singlePartMIMEReaderCallbacks.size(), 3);
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(0)._finishedData, _bodyB.getPartData());
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(0)._headers, _bodyB.getPartHeaders());
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(1)._finishedData, _bodyC.getPartData());
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(1)._headers, _bodyC.getPartHeaders());
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(2)._finishedData, _bodyD.getPartData());
        Assert.assertEquals(singlePartMIMEReaderCallbacks.get(2)._headers, _bodyD.getPartHeaders());
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
        boolean _firstPartEchoed = false;
        final Callback<StreamResponse> _callback;
        final List<ServerSinglePartMIMEReader> _singlePartMIMEReaderCallbacks =
                new ArrayList<ServerSinglePartMIMEReader>();

        @Override
        public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader) {
            if (!_firstPartEchoed) {
                _firstPartEchoed = true;
                final MultiPartMIMEWriter writer =
                        new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder().appendDataSource(singleParMIMEReader).build();

                final StreamResponse streamResponse = mock(StreamResponse.class);
                when(streamResponse.getEntityStream()).thenReturn(writer.getEntityStream());
                final String contentTypeHeader = "multipart/mixed; boundary=" + writer.getBoundary();
                when(streamResponse.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);
                _callback.onSuccess(streamResponse);


            } else {
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

        ServerMultiPartMIMEReaderSinglePartEcho(final CountDownLatch latch, final Callback<StreamResponse> callback) {
            _latch = latch;
            _callback = callback;
        }
    }


}
