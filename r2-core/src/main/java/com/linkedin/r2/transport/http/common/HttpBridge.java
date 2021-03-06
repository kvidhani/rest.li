/*
   Copyright (c) 2012 LinkedIn Corp.

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

/* $Id$ */
package com.linkedin.r2.transport.http.common;


import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponse;
import com.linkedin.r2.transport.common.bridge.common.TransportResponseImpl;

import java.net.URI;
import java.util.Map;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public class HttpBridge
{
  /**
   * Wrap application callback for incoming RestResponse with a "generic" HTTP callback.
   *
   * @param callback the callback to receive the incoming RestResponse
   * @param request the request, used only to provide useful context in case an error
   *          occurs
   * @return the callback to receive the incoming HTTP response
   */
  public static TransportCallback<StreamResponse> streamToHttpCallback(final TransportCallback<StreamResponse> callback,
                                                        StreamRequest request)
  {
    final URI uri = request.getURI();
    return new TransportCallback<StreamResponse>()
    {
      @Override
      public void onResponse(TransportResponse<StreamResponse> response)
      {
        if (response.hasError())
        {
          response =
              TransportResponseImpl.error(new RemoteInvocationException("Failed to get response from server for URI "
                                                                            + uri,
                                                                        response.getError()),
                                          response.getWireAttributes());
        }
        else if (!RestStatus.isOK(response.getResponse().getStatus()))
        {
          response =
              TransportResponseImpl.error(new StreamException(response.getResponse(),
                                                            "Received error "
                                                                + response.getResponse()
                                                                          .getStatus()
                                                                + " from server for URI "
                                                                + uri),
                                          response.getWireAttributes());
        }

        callback.onResponse(response);
      }
    };
  }

  public static StreamRequest toStreamRequest(StreamRequest request, Map<String, String> headers)
  {
    return request.builder()
        .unsafeSetHeaders(headers)
        .build(request.getEntityStream());
  }

  /**
   * Wrap transport callback for outgoing "generic" http response with a callback to pass
   * to the application REST server.
   *
   * @param callback the callback to receive the outgoing HTTP response
   * @return the callback to receive the outgoing REST response
   */
  public static TransportCallback<StreamResponse> httpToStreamCallback(final TransportCallback<StreamResponse> callback)
  {
    return new TransportCallback<StreamResponse>()
    {
      @Override
      public void onResponse(TransportResponse<StreamResponse> response)
      {
        if (response.hasError())
        {
          final Throwable ex = response.getError();
          if (ex instanceof StreamException)
          {
            callback.onResponse(TransportResponseImpl.success(((StreamException) ex).getResponse(),
                                                              response.getWireAttributes()));
            return;
          }
        }

        callback.onResponse(response);
      }
    };
  }
}
