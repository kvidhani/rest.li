package com.linkedin.multipart;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.rest.StreamResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.server.HttpServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import test.r2.integ.AbstractStreamTest;

/**
 * Abstract class for async multipart mime integration tests.
 *
 * @author Karim Vidhani
 */
public abstract class AbstractMIMEIntegrationStreamTest {
    protected static final int PORT = 8388;
    protected static final int TEST_TIMEOUT = 30000;
    protected HttpServer _server;
    protected TransportClientFactory _clientFactory;
    protected Client _client;

    @BeforeMethod
    public void setup() throws IOException
    {
        _clientFactory = getClientFactory();
        _client = new TransportClientAdapter(_clientFactory.getClient(getClientProperties()));
        _server = getServerFactory().createServer(PORT, getTransportDispatcher());
        _server.start();
    }

    @AfterMethod
    public void tearDown() throws Exception
    {
        final FutureCallback<None> clientShutdownCallback = new FutureCallback<None>();
        _client.shutdown(clientShutdownCallback);
        clientShutdownCallback.get();

        final FutureCallback<None> factoryShutdownCallback = new FutureCallback<None>();
        _clientFactory.shutdown(factoryShutdownCallback);
        factoryShutdownCallback.get();

        if (_server != null) {
            _server.stop();
            _server.waitForStop();
        }
    }

    protected abstract TransportDispatcher getTransportDispatcher();

    protected TransportClientFactory getClientFactory()
    {
        return new HttpClientFactory();
    }

    protected Map<String, String> getClientProperties()
    {
        return Collections.emptyMap();
    }

    protected HttpServerFactory getServerFactory()
    {
        return new HttpServerFactory();
    }

    protected static Callback<StreamResponse> expectSuccessCallback(final CountDownLatch latch,
                                                                    final AtomicInteger status,
                                                                    final Map<String, String> headers)
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
                headers.putAll(result.getHeaders());
                latch.countDown();
            }
        };
    }
}