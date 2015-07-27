package com.linkedin.multipart.integ;

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
 * Created by kvidhani on 7/11/15.
 */

//in rb mention:
//        1. using writer directly in your interface - i.e composite writer
//        2. the fact you are using the package private interface to hide public api details - could have also used abstract class….
//        3. the writer has three constructors (part, single and multi)



public abstract class AbstractMultiPartMIMEIntegrationStreamTest  {

    protected static final int PORT = 8388;
    protected HttpServer _server;
    protected TransportClientFactory _clientFactory;
    protected Client _client;
    protected ScheduledExecutorService _scheduler;

    @BeforeMethod
    public void setup() throws IOException
    {
        _scheduler = Executors.newSingleThreadScheduledExecutor();
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

        _scheduler.shutdown();
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
