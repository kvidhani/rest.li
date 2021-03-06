package com.linkedin.r2.filter.transport;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.StreamRequestFilter;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.QueryTunnelUtil;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;

import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public class ClientQueryTunnelFilter implements StreamRequestFilter
{
  private final int _queryPostThreshold;

  public ClientQueryTunnelFilter(int queryPostThreshold)
  {
    _queryPostThreshold = queryPostThreshold;
  }

  @Override
  public void onRequest(final StreamRequest req,
                     final RequestContext requestContext,
                     final Map<String, String> wireAttrs,
                     final NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    Callback<StreamRequest> callback = new Callback<StreamRequest>()
    {
      @Override
      public void onError(Throwable e)
      {
        nextFilter.onError(e, requestContext, wireAttrs);
      }

      @Override
      public void onSuccess(StreamRequest newReq)
      {
        nextFilter.onRequest(newReq, requestContext, wireAttrs);
      }
    };

    QueryTunnelUtil.encode(req, requestContext, _queryPostThreshold, callback);
  }
}
