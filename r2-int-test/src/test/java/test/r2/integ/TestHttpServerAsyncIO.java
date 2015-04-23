package test.r2.integ;

import com.linkedin.r2.transport.http.server.HttpJettyServer;
import com.linkedin.r2.transport.http.server.HttpServerFactory;

/**
 * @author Zhenkai Zhu
 */
public class TestHttpServerAsyncIO extends TestHttpServer
{
  protected HttpServerFactory getServerFactory()
  {
    return new HttpServerFactory(HttpJettyServer.ServletType.ASYNC_IO);
  }

}
