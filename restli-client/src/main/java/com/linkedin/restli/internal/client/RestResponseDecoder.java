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
import com.linkedin.multipart.MultiPartMIMEReader;
import com.linkedin.multipart.MultiPartMIMEReaderCallback;
import com.linkedin.multipart.SinglePartMIMEReaderCallback;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.r2.message.stream.entitystream.FullEntityReader;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestLiDecodingException;
import com.linkedin.restli.common.ProtocolVersion;
import com.linkedin.restli.common.RestConstants;
import com.linkedin.restli.common.attachments.RestLiAttachmentReader;
import com.linkedin.restli.internal.common.AllProtocolVersions;
import com.linkedin.restli.internal.common.CookieUtil;
import com.linkedin.restli.internal.common.DataMapConverter;
import com.linkedin.restli.internal.common.ProtocolVersionUtil;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.activation.MimeTypeParseException;
import javax.mail.internet.ContentType;
import javax.mail.internet.ParseException;


/**
 * Converts a raw RestResponse or a StreamResponse into a type-bound response.  The class is abstract
 * and must be subclassed according to the expected response type.
 *
 * If the StreamResponse contains attachments, then the first part is read in and used to create the response.
 *
 * @author Steven Ihde
 * @author Karim Vidhani
 *
 * @version $Revision: $
 */
public abstract class RestResponseDecoder<T>
{
  public void decodeResponse(final StreamResponse streamResponse, final Callback<Response<T>> responseCallback) throws RestLiDecodingException
  {
    //Determine content type and take appropriate action.
    //If 'multipart/related', then use MultiPartMIMEReader to read first part (which can be json or pson).
    final String contentTypeString = streamResponse.getHeader(RestConstants.HEADER_CONTENT_TYPE);
    if(contentTypeString != null)
    {
      ContentType contentType = null;
      try
      {
        contentType = new ContentType(contentTypeString);
      }
      catch (ParseException parseException)
      {
        responseCallback.onError(new RestLiDecodingException("Could not decode Content-Type header in response", parseException));
      }
      if(contentType.getBaseType().equalsIgnoreCase(RestConstants.HEADER_VALUE_MULTIPART_RELATED))
      {
        final MultiPartMIMEReader multiPartMIMEReader = MultiPartMIMEReader.createAndAcquireStream(streamResponse);
        final TopLevelReaderCallback firstPartReader = new TopLevelReaderCallback(responseCallback, streamResponse, multiPartMIMEReader);
        multiPartMIMEReader.registerReaderCallback(firstPartReader);
        return;
      }
    }

    //Otherwise if the whole body is json/pson then read everything in.
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
          responseCallback.onSuccess(createResponse(streamResponse.getHeaders(), streamResponse.getStatus(), result, streamResponse.getCookies()));
        }
        catch (Exception exception)
        {
          onError(exception);
        }
      }
    });
    streamResponse.getEntityStream().setReader(fullEntityReader);
  }

  public Response<T> decodeResponse(RestResponse restResponse) throws RestLiDecodingException
  {
    return createResponse(restResponse.getHeaders(), restResponse.getStatus(), restResponse.getEntity(), restResponse.getCookies());
  }

  private ResponseImpl<T> createResponse(Map<String, String> headers, int status, ByteString entity, List<String> cookies)
      throws RestLiDecodingException
  {
    ResponseImpl<T> response = new ResponseImpl<T>(status, headers, CookieUtil.decodeSetCookies(cookies));

    try
    {
      DataMap dataMap = (entity.isEmpty()) ? null : DataMapConverter.bytesToDataMap(headers, entity);
      response.setEntity(wrapResponse(dataMap, headers, ProtocolVersionUtil.extractProtocolVersion(response.getHeaders())));
      return response;
    }
    catch (MimeTypeParseException e)
    {
      throw new RestLiDecodingException("Could not decode REST response", e);
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

    private void setResponse(ResponseImpl<T> response)
    {
      _response = response;
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
        //This is the 2nd part, so pass this on to the client.
        _response.setAttachmentReader(new RestLiAttachmentReader(_multiPartMIMEReader));
        _responseCallback.onSuccess(_response);
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
        _topLevelReaderCallback
            .setResponse(createResponse(_streamResponse.getHeaders(), _streamResponse.getStatus(), _builder.build(), _streamResponse.getCookies()));
        //Note that we can't answer the callback of the client yet since we don't know if there are more parts.
      }
      catch (Exception exception)
      {
        _topLevelReaderCallback.onStreamError(exception);
      }
    }

    @Override
    public void onAbandoned()
    {
      _topLevelReaderCallback.onStreamError(new IllegalStateException(
          "Serious error. There should never be a call to abandon"
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

  /**
   * This method is public to accommodate a small number of external users.  However, to make these use cases more
   * stable we plan on eventually removing this method or disallowing public access. Therefore, external users should
   * preferably not depend on this method.
   *
   * Wraps the given DataMap into its proper response type using the protocol version specified.
   *
   * @param dataMap the json body of the response
   * @param headers the response headers
   * @param version the protocol version
   * @return the response
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   * @throws NoSuchMethodException
   * @throws IOException
   */
  public abstract T wrapResponse(DataMap dataMap, Map<String, String> headers, ProtocolVersion version)
                  throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException;
}
