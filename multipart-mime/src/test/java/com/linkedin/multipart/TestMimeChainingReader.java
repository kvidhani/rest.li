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

//todo file radar about top level not called when tis passed on
    //refr to comment in reader callback way below

public class TestMimeChainingReader {

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


    //Verifies that a multi part mime reader can be used as a data source to the writer.
    //To make the test easier to write, we simply chain back to the client in the form of simulating a response.

    //This test creates a multipart mime request. On the server side, upon invocation to onNewPart(), the server
    //creates a writer to send back the entire reader first part.
    @DataProvider(name = "chunkSizes")
    public Object[][] chunkSizes() throws Exception {
        return new Object[][]{
                {1},
                {R2Constants.DEFAULT_DATA_CHUNK_SIZE}
        };
    }

    @Test(dataProvider = "chunkSizes")
    public void testMimeReaderDataSource(final int chunkSize) throws Exception {

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

        //Client side preparation to read the part back on the callback
        //Note the chunks size will carry over since the client is controlling how much data he gets back
        //based on the chunk size he writes.
        final CountDownLatch latch = new CountDownLatch(1);
        ClientMultiPartMIMEReaderEchoReceiver _clientReceiver =
                new ClientMultiPartMIMEReaderEchoReceiver(latch);
        Callback<StreamResponse> callback = expectSuccessChainCallback(_clientReceiver);

        //Server side start
        MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
        ServerMultiPartMIMEReaderSinglePartEcho _serverSender =
                new ServerMultiPartMIMEReaderSinglePartEcho(callback, reader);
        reader.registerReaderCallback(_serverSender);

        latch.await(60000, TimeUnit.MILLISECONDS);


        //Verify client. No need to verify the server.
        List<ClientSinglePartMIMEReaderEchoReceiver> singlePartMIMEReaderCallbacks =
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
        final CountDownLatch _latch;

        @Override
        public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader) {
            ClientSinglePartMIMEReaderEchoReceiver singlePartMIMEReaderCallback = new ClientSinglePartMIMEReaderEchoReceiver(singleParMIMEReader);
            singleParMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
            _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
            singleParMIMEReader.requestPartData();
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

        ClientMultiPartMIMEReaderEchoReceiver(final CountDownLatch latch) {
            _latch = latch;
        }
    }


    //Server callback. Only the top level needed:
    private static class ServerMultiPartMIMEReaderSinglePartEcho implements MultiPartMIMEReaderCallback {

        final Callback<StreamResponse> _callback;
        final MultiPartMIMEReader _reader;

        @Override
        public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader) {

                final MultiPartMIMEWriter writer =
                        new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder().appendMultiPartDataSource(_reader).build();
                final StreamResponse streamResponse = mock(StreamResponse.class);
                when(streamResponse.getEntityStream()).thenReturn(writer.getEntityStream());
                final String contentTypeHeader = "multipart/mixed; boundary=" + writer.getBoundary();
                when(streamResponse.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);
                _callback.onSuccess(streamResponse);

        }

        @Override
        public void onFinished() {
            //Based on the current implementation, this will not be called.
            System.out.println("a");
        }

        @Override
        public void onAbandoned() {
            Assert.fail();
        }

        @Override
        public void onStreamError(Throwable e) {
            Assert.fail();
        }

        ServerMultiPartMIMEReaderSinglePartEcho(final Callback<StreamResponse> callback,
                                                final MultiPartMIMEReader reader) {
            _callback = callback;
            _reader = reader;
        }
    }


}
