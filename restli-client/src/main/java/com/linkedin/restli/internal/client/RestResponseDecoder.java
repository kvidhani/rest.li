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
import com.linkedin.multipart.MultiPartMIMEReader;
import com.linkedin.multipart.MultiPartMIMEReaderCallback;
import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.streaming.FullEntityReader;
import com.linkedin.restli.client.RestLiAttachmentReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;

import com.linkedin.data.ByteString;
import com.linkedin.data.DataMap;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.data.codec.PsonDataCodec;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestLiDecodingException;
import com.linkedin.restli.common.ProtocolVersion;
import com.linkedin.restli.common.RestConstants;
import com.linkedin.restli.internal.common.AllProtocolVersions;
import com.linkedin.restli.internal.common.ProtocolVersionUtil;

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

  public Response<T> decodeResponse(StreamResponse streamResponse) throws RestLiDecodingException
  {
    //Determine content type first.
    //If 'multipart/related', then use MultiPartMIMEReader to read first part (which can be json or pson).
    //Otherwise if the whole body is json/pson then read everything in.
    if(streamResponse.getHeader(RestConstants.HEADER_CONTENT_TYPE).equalsIgnoreCase(RestConstants.HEADER_VALUE_MULTIPART_RELATED))
    {
      final FirstPartReader firstPartReader = new FirstPartReader();
      final MultiPartMIMEReader multiPartMIMEReader = MultiPartMIMEReader.createAndAcquireStream(streamResponse, firstPartReader);
    }
    else
    {
      final FullEntityReader fullEntityReader = new FullEntityReader(new Callback<ByteString>()
      {
        @Override
        public void onError(Throwable e)
        {
          //todo
        }

        @Override
        public void onSuccess(ByteString result)
        {
        }
      });
    }
  }

  public Response<T> decodeResponse(RestResponse restResponse) throws RestLiDecodingException
  {
    return createResponse(restResponse.getHeaders(), restResponse.getStatus(), restResponse.getEntity());
  }

  private Response<T> createResponse(Map<String, String> headers, int status, ByteString entity) throws RestLiDecodingException
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

  private class FirstPartReader implements MultiPartMIMEReaderCallback
  {

    /**
     * Invoked (at some time in the future) upon a registration with a {@link com.linkedin.multipart.MultiPartMIMEReader}.
     * Also invoked when previous parts are finished and new parts are available.
     *

     * @param singleParMIMEReader the SinglePartMIMEReader which can be used to walk through this part.
     */
    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
    {

    }

    /**
     * Invoked when this reader is finished and the multipart mime envelope has been completely read.
     */
    @Override
    public void onFinished()
    {

    }

    /**
     * Invoked as a result of calling {@link com.linkedin.multipart.MultiPartMIMEReader#abandonAllParts()}. This will be invoked
     * at some time in the future when all the parts from this multipart mime envelope are abandoned.
     */
    @Override
    public void onAbandoned()
    {

    }

    /**
     * Invoked when there was an error reading from the multipart envelope.
     *

     * @param throwable the Throwable that caused this to happen.
     */
    @Override
    public void onStreamError(Throwable throwable)
    {

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
