package test.r2.integ;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.common.bridge.server.StreamDispatcher;
import com.linkedin.r2.transport.common.bridge.server.StreamDispatcherBuilder;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.server.HttpServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Zhenkai Zhu
 */
public abstract class AbstractStreamTest
{
  protected HttpClientFactory _clientFactory;
  protected static final int PORT = 8088;
  protected static final long LARGE_BYTES_NUM = 1024 * 1024 * 1024;
  protected static final long SMALL_BYTES_NUM = 1024 * 1024 * 32;
  protected static final long TINY_BYTES_NUM = 1024 * 64;
  protected static final byte BYTE = 100;
  protected static final long INTERVAL = 20;
  protected HttpServer _server;
  protected Client _client;
  protected ScheduledExecutorService _scheduler;

  @BeforeSuite
  public void setup() throws IOException
  {
    _scheduler = Executors.newSingleThreadScheduledExecutor();
    _clientFactory = new HttpClientFactory();
    _client = new TransportClientAdapter(_clientFactory.getClient(getClientProperties()));
    _server = new HttpServerFactory().createStreamServer(PORT, getStreamDispatcher());
    _server.start();
  }

  @AfterSuite
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

  protected abstract StreamDispatcher getStreamDispatcher();

  protected Map<String, String> getClientProperties()
  {
    return Collections.emptyMap();
  }

}