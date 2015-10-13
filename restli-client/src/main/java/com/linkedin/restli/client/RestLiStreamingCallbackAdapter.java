package com.linkedin.restli.client;


import com.linkedin.common.callback.Callback;
import com.linkedin.restli.internal.client.RestResponseDecoder;


/**
 * Converts StreamResponse into Response and different exceptions -> RemoteInvocationException.
 * @param <T> response type
 */
public class RestLiStreamingCallbackAdapter<T> implements Callback<Response<T>> //<T> extends CallbackAdapter<Response<T>, StreamResponse>
{
  private final RestResponseDecoder<T> _decoder;

  public RestLiStreamingCallbackAdapter(RestResponseDecoder<T> decoder, Callback<Response<T>> callback)
  {
    _decoder = decoder;
  }

  /*
  @Override
  protected Response<T> convertResponse(StreamResponse response) throws Exception
  {
    return _decoder.decodeResponse(response);
  }

  @Override
  protected Throwable convertError(Throwable error)
  {
    return ExceptionUtil.exceptionForThrowable(error, _decoder);
  }*/

  @Override
  public void onError(Throwable e)
  {

  }

  @Override
  public void onSuccess(Response<T> result)
  {

  }
}
