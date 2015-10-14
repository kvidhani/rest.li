package com.linkedin.restli.client;


import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.restli.internal.client.RestResponseDecoder;

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