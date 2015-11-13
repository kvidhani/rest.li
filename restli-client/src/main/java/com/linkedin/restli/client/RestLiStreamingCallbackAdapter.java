/*
   Copyright (c) 2014 LinkedIn Corp.

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

package com.linkedin.restli.client;


import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.restli.internal.client.RestResponseDecoder;


/**
 * @author Karim Vidhani
 */
final class RestLiStreamingCallbackAdapter<T> implements Callback<StreamResponse>
{
  private final Callback<Response<T>> _wrappedCallback;
  private final RestResponseDecoder<T> _decoder;

  public RestLiStreamingCallbackAdapter(RestResponseDecoder<T> decoder, Callback<Response<T>> wrappedCallback)
  {
    _decoder = decoder;
    _wrappedCallback = wrappedCallback;
  }

  @Override
  public void onError(Throwable e)
  {
    _wrappedCallback.onError(e);
  }

  @Override
  public void onSuccess(StreamResponse result)
  {
    try
    {
      _decoder.decodeResponse(result, _wrappedCallback);
    }
    catch(Exception exception)
    {
      onError(exception);
    }
  }
}