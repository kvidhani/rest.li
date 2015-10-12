package com.linkedin.restli.client;


import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.CallbackAdapter;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.restli.internal.client.ExceptionUtil;
import com.linkedin.restli.internal.client.RestResponseDecoder;


/**
 * Converts StreamResponse into Response and different exceptions -> RemoteInvocationException.
 * @param <T> response type
 */
public class RestLiStreamingCallbackAdapter<T> extends CallbackAdapter<Response<T>, StreamResponse>
{
  private final RestResponseDecoder<T> _decoder;

  public RestLiStreamingCallbackAdapter(RestResponseDecoder<T> decoder, Callback<Response<T>> callback)
  {
    super(callback);
    _decoder = decoder;
  }

  @Override
  protected Response<T> convertResponse(StreamResponse response) throws Exception
  {

    return _decoder.decodeResponse(response);
  }

  @Override
  protected Throwable convertError(Throwable error)
  {
    return ExceptionUtil.exceptionForThrowable(error, _decoder);
  }
}
