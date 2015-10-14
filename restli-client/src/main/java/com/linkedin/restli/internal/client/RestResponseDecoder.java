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

/**
 * $Id: $
 */

package com.linkedin.restli.internal.client;

import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.data.DataMap;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.data.codec.PsonDataCodec;
import com.linkedin.multipart.MultiPartMIMEReader;
import com.linkedin.multipart.MultiPartMIMEReaderCallback;
import com.linkedin.multipart.SinglePartMIMEReaderCallback;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.FullEntityReader;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestLiAttachmentReader;
import com.linkedin.restli.client.RestLiDecodingException;
import com.linkedin.restli.common.ProtocolVersion;
import com.linkedin.restli.common.RestConstants;
import com.linkedin.restli.internal.common.AllProtocolVersions;
import com.linkedin.restli.internal.common.ProtocolVersionUtil;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;

/**
 * Converts a raw RestResponse into a type-bound response.  The class is abstract
 * and must be subclassed according to the expected response type.
 * @author Steven Ihde
 * @version $Revision: $
 */
public abstract class RestResponseDecoder<T>
{
  private static final JacksonDataCodec JACKSON_DATA_CODEC = new JacksonDataCodec();
  private static final PsonDataCodec    PSON_DATA_CODEC    = new PsonDataCodec();

  public void decodeResponse(final StreamResponse streamResponse, final Callback<Response<T>> responseCallback) throws RestLiDecodingException
  {
    //Determine content type and take appropriate action.
    //If 'multipart/related', then use MultiPartMIMEReader to read first part (which can be json or pson).
    //Otherwise if the whole body is json/pson then read everything in.
    if(streamResponse.getHeader(RestConstants.HEADER_CONTENT_TYPE).equalsIgnoreCase(RestConstants.HEADER_VALUE_MULTIPART_RELATED))
    {
      final MultiPartMIMEReader multiPartMIMEReader = MultiPartMIMEReader.createAndAcquireStream(streamResponse);
      final TopLevelReaderCallback firstPartReader = new TopLevelReaderCallback(responseCallback, streamResponse, multiPartMIMEReader);
      multiPartMIMEReader.registerReaderCallback(firstPartReader);
    }
    else
    {
      //This will not have an extra copy due to assembly since FullEntityReader uses a compound ByteString.
      final FullEntityReader fullEntityReader = new FullEntityReader(new Callback<ByteString>()
      {
        @Override
        public void onError(Throwable e)
        {
          responseCallback.onError(e);
        }

        @Override
        public void onSuccess(ByteString result)
        {
          try
          {
            responseCallback.onSuccess(createResponse(streamResponse.getHeaders(), streamResponse.getStatus(), result));
          }
          catch (Exception exception)
          {
            onError(exception);
          }
        }
      });
      streamResponse.getEntityStream().setReader(fullEntityReader);
    }
  }

  public Response<T> decodeResponse(RestResponse restResponse) throws RestLiDecodingException
  {
    return createResponse(restResponse.getHeaders(), restResponse.getStatus(), restResponse.getEntity());
  }

  private ResponseImpl<T> createResponse(Map<String, String> headers, int status, ByteString entity) throws RestLiDecodingException
  {
    ResponseImpl<T> response = new ResponseImpl<T>(status, headers);

    try
    {
      DataMap dataMap;
      if (entity.length() == 0)
      {
        dataMap = null;
      }
      else
      {
        InputStream inputStream = entity.asInputStream();
        if ((RestConstants.HEADER_VALUE_APPLICATION_PSON)
            .equalsIgnoreCase(headers.get(RestConstants.HEADER_CONTENT_TYPE)))
        {
          dataMap = PSON_DATA_CODEC.readMap(inputStream);
        }
        else
        {
          dataMap = JACKSON_DATA_CODEC.readMap(inputStream);
        }
        inputStream.close();
      }
      response.setEntity(wrapResponse(dataMap, headers, ProtocolVersionUtil.extractProtocolVersion(response.getHeaders())));
      return response;
    }
    catch (IOException e)
    {
      throw new RestLiDecodingException("Could not decode REST response", e);
    }
    catch (InstantiationException e)
    {
      throw new IllegalStateException(e);
    }
    catch (IllegalAccessException e)
    {
      throw new IllegalStateException(e);
    }
    catch (InvocationTargetException e)
    {
      throw new IllegalStateException(e);
    }
    catch (NoSuchMethodException e)
    {
      throw new IllegalStateException(e);
    }
  }

  private class TopLevelReaderCallback implements MultiPartMIMEReaderCallback
  {
    private final Callback<Response<T>> _responseCallback;
    private final StreamResponse _streamResponse;
    private final MultiPartMIMEReader _multiPartMIMEReader;
    private ResponseImpl<T> _response = null;

    private TopLevelReaderCallback(final Callback<Response<T>> responseCallback,
                                   final StreamResponse streamResponse,
                                   final MultiPartMIMEReader multiPartMIMEReader)
    {
      _responseCallback = responseCallback;
      _streamResponse = streamResponse;
      _multiPartMIMEReader = multiPartMIMEReader;
    }

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
    {
      if (_response == null)
      {
        //The first time
        FirstPartReaderCallback firstPartReaderCallback = new FirstPartReaderCallback(_responseCallback,
            this,
            _multiPartMIMEReader,
            singleParMIMEReader,
            _streamResponse);
        singleParMIMEReader.registerReaderCallback(firstPartReaderCallback);
        singleParMIMEReader.requestPartData();
      }
      else
      {
        //This is the 2nd part, so pass this on to the client
        _response.setAttachmentReader(new RestLiAttachmentReader(_multiPartMIMEReader));
      }
    }

    @Override
    public void onFinished()
    {
      //Verify we actually had some parts
      if (_response == null)
      {
        _responseCallback.onError(new RemoteInvocationException("Did not receive any parts in the multipart mime response!"));
      }

      //At this point, this means that the multipart mime envelope didn't have any attachments (apart from the
      //json/pson payload).
      //In this case we set the attachment reader to null.
      _response.setAttachmentReader(null);
      _responseCallback.onSuccess(_response);
    }

    @Override
    public void onAbandoned()
    {
      _responseCallback.onError(new IllegalStateException("Serious error. There should never be a call to abandon"
          + " the entire payload when decoding a multipart mime response."));
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      _responseCallback.onError(throwable);
    }
  }

  private class FirstPartReaderCallback implements SinglePartMIMEReaderCallback
  {
    private final Callback<Response<T>> _responseCallback;
    private final MultiPartMIMEReader _multiPartMIMEReader;
    private final TopLevelReaderCallback _topLevelReaderCallback;
    private final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    private final StreamResponse _streamResponse;
    private final ByteString.Builder _builder = new ByteString.Builder();
    private ResponseImpl<T> _response = null;

    public FirstPartReaderCallback(final Callback<Response<T>> responseCallback,
                                   final TopLevelReaderCallback topLevelReaderCallback,
                                   final MultiPartMIMEReader multiPartMIMEReader,
                                   final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader,
                                   final StreamResponse streamResponse)
    {
      _responseCallback = responseCallback;
      _topLevelReaderCallback = topLevelReaderCallback;
      _multiPartMIMEReader = multiPartMIMEReader;
      _singlePartMIMEReader = singlePartMIMEReader;
      _streamResponse = streamResponse;
    }

    @Override
    public void onPartDataAvailable(ByteString partData)
    {
      _builder.append(partData);
      _singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished()
    {
      try
      {
        _response = createResponse(_streamResponse.getHeaders(), _streamResponse.getStatus(), _builder.build());
        //Note that we can't answer the callback of the client yet since we don't know if there are more parts. Hence
      } catch (Exception exception)
      {
        _topLevelReaderCallback.onStreamError(exception);
      }
    }

    @Override
    public void onAbandoned()
    {
      _responseCallback.onError(new IllegalStateException("Serious error. There should never be a call to abandon"
          + " part data when decoding the first part in a multipart mime response."));
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      //No need to do anything as the MultiPartMIMEReader will also call onStreamError() on the top level callback
      //which will then call the response callback.
    }
  }

  public abstract Class<?> getEntityClass();

  /**
   * @deprecated use {@link #wrapResponse(com.linkedin.data.DataMap, java.util.Map, com.linkedin.restli.common.ProtocolVersion)}
   */
  @Deprecated
  public T wrapResponse(DataMap dataMap)
                  throws InvocationTargetException, NoSuchMethodException, InstantiationException, IOException, IllegalAccessException
  {
    return wrapResponse(dataMap, Collections.<String, String>emptyMap(), AllProtocolVersions.RESTLI_PROTOCOL_1_0_0.getProtocolVersion());
  }

  protected abstract T wrapResponse(DataMap dataMap, Map<String, String> headers, ProtocolVersion version)
                  throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException;
}
