/*
   Copyright (c) 2015 LinkedIn Corp.

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

package com.linkedin.multipart;


import com.linkedin.r2.message.rest.StreamResponse;
import com.linkedin.r2.message.rest.StreamResponseBuilder;

import java.util.List;
import java.util.Map;


/**
 * A wrapper to enforce creating a proper multipart mime {@link com.linkedin.r2.message.rest.StreamResponse}
 *
 * @author Karim Vidhani
 */
public class MultiPartMIMEStreamResponseBuilder
{
  private final String _mimeType;
  private final Map<String, String> _contentTypeParameters;
  private final MultiPartMIMEWriter _writer;
  private final String _boundary;
  private final StreamResponseBuilder _streamResponseBuilder;

  public MultiPartMIMEStreamResponseBuilder(final String mimeType, final MultiPartMIMEWriter writer,
      final Map<String, String> contentTypeParameters)
  {
    _mimeType = mimeType.trim();
    _writer = writer;
    _boundary = writer.getBoundary();
    _contentTypeParameters = contentTypeParameters;
    _streamResponseBuilder = new StreamResponseBuilder();
  }

  public StreamResponse build()
  {
    final String contentTypeHeader =
        MultiPartMIMEUtils.buildMIMEContentTypeHeader(_mimeType, _boundary, _contentTypeParameters);
    _streamResponseBuilder.addHeaderValue(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, contentTypeHeader);
    return _streamResponseBuilder.build(_writer.getEntityStream());
  }

  public StreamResponse buildCanonical()
  {
    final String contentTypeHeader =
        MultiPartMIMEUtils.buildMIMEContentTypeHeader(_mimeType, _boundary, _contentTypeParameters);
    _streamResponseBuilder.addHeaderValue(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, contentTypeHeader);
    return _streamResponseBuilder.buildCanonical(_writer.getEntityStream());
  }

  public MultiPartMIMEStreamResponseBuilder setStatus(final int status)
  {
    _streamResponseBuilder.setStatus(status);
    return this;
  }

  public int getStatus()
  {
    return _streamResponseBuilder.getStatus();
  }

  public MultiPartMIMEStreamResponseBuilder setHeaders(final Map<String, String> headers)
  {
    _streamResponseBuilder.setHeaders(headers);
    return this;
  }

  public MultiPartMIMEStreamResponseBuilder setHeader(final String name, final String value)
  {
    _streamResponseBuilder.setHeader(name, value);
    return this;
  }

  public MultiPartMIMEStreamResponseBuilder addHeaderValue(final String name, final String value)
  {
    _streamResponseBuilder.addHeaderValue(name, value);
    return this;
  }

  public MultiPartMIMEStreamResponseBuilder overwriteHeaders(final Map<String, String> headers)
  {
    _streamResponseBuilder.overwriteHeaders(headers);
    return this;
  }

  public MultiPartMIMEStreamResponseBuilder addCookie(final String cookie)
  {
    _streamResponseBuilder.addCookie(cookie);
    return this;
  }

  public MultiPartMIMEStreamResponseBuilder setCookies(final List<String> cookies)
  {
    _streamResponseBuilder.setCookies(cookies);
    return this;
  }

  public MultiPartMIMEStreamResponseBuilder clearHeaders()
  {
    _streamResponseBuilder.clearHeaders();
    return this;
  }

  public MultiPartMIMEStreamResponseBuilder clearCookies()
  {
    _streamResponseBuilder.clearCookies();
    return this;
  }

  public Map<String, String> getHeaders()
  {
    return _streamResponseBuilder.getHeaders();
  }

  public List<String> getCookies()
  {
    return _streamResponseBuilder.getCookies();
  }

  public String getHeader(final String name)
  {
    return _streamResponseBuilder.getHeader(name);
  }

  public List<String> getHeaderValues(final String name)
  {
    return _streamResponseBuilder.getHeaderValues(name);
  }

  public MultiPartMIMEStreamResponseBuilder unsafeSetHeader(final String name, final String value)
  {
    _streamResponseBuilder.unsafeSetHeader(name, value);
    return this;
  }

  public MultiPartMIMEStreamResponseBuilder unsafeAddHeaderValue(final String name, final String value)
  {
    _streamResponseBuilder.unsafeAddHeaderValue(name, value);
    return this;
  }

  public MultiPartMIMEStreamResponseBuilder unsafeSetHeaders(final Map<String, String> headers)
  {
    _streamResponseBuilder.unsafeSetHeaders(headers);
    return this;
  }

  public MultiPartMIMEStreamResponseBuilder unsafeOverwriteHeaders(final Map<String, String> headers)
  {
    _streamResponseBuilder.unsafeOverwriteHeaders(headers);
    return this;
  }
}