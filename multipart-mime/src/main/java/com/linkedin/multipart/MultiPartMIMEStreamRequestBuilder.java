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


import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamRequestBuilder;

import java.net.URI;
import java.util.List;
import java.util.Map;


/**
 * A wrapper to enforce creating a proper multipart mime{@link com.linkedin.r2.message.rest.StreamRequest}
 *
 * @author Karim Vidhani
 */
public final class MultiPartMIMEStreamRequestBuilder
{
  private final String _mimeType;
  private final Map<String, String> _contentTypeParameters;
  private final MultiPartMIMEWriter _writer;
  private final String _boundary;
  private final StreamRequestBuilder _streamRequestBuilder;

  public MultiPartMIMEStreamRequestBuilder(final URI uri, final String mimeType, final MultiPartMIMEWriter writer,
      final Map<String, String> contentTypeParameters)
  {
    _mimeType = mimeType.trim();
    _writer = writer;
    _boundary = writer.getBoundary();
    _contentTypeParameters = contentTypeParameters;
    _streamRequestBuilder = new StreamRequestBuilder(uri);
  }

  public StreamRequest build()
  {
    final String contentTypeHeader =
        MultiPartMIMEUtils.buildMIMEContentTypeHeader(_mimeType, _boundary, _contentTypeParameters);
    _streamRequestBuilder.addHeaderValue(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, contentTypeHeader);
    return _streamRequestBuilder.build(_writer.getEntityStream());
  }

  public StreamRequest buildCanonical()
  {
    final String contentTypeHeader =
        MultiPartMIMEUtils.buildMIMEContentTypeHeader(_mimeType, _boundary, _contentTypeParameters);
    _streamRequestBuilder.addHeaderValue(MultiPartMIMEUtils.CONTENT_TYPE_HEADER, contentTypeHeader);
    return _streamRequestBuilder.buildCanonical(_writer.getEntityStream());
  }

  public URI getURI()
  {
    return _streamRequestBuilder.getURI();
  }

  public MultiPartMIMEStreamRequestBuilder setURI(final URI uri)
  {
    _streamRequestBuilder.setURI(uri);
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder setMethod(final String method)
  {
    _streamRequestBuilder.setMethod(method);
    return this;
  }

  public String getMethod()
  {
    return _streamRequestBuilder.getMethod();
  }

  public MultiPartMIMEStreamRequestBuilder setHeaders(final Map<String, String> headers)
  {
    _streamRequestBuilder.setHeaders(headers);
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder setHeader(final String name, final String value)
  {
    _streamRequestBuilder.setHeader(name, value);
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder addHeaderValue(final String name, final String value)
  {
    _streamRequestBuilder.addHeaderValue(name, value);
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder overwriteHeaders(final Map<String, String> headers)
  {
    _streamRequestBuilder.overwriteHeaders(headers);
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder addCookie(final String cookie)
  {
    _streamRequestBuilder.addCookie(cookie);
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder setCookies(final List<String> cookies)
  {
    _streamRequestBuilder.setCookies(cookies);
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder clearHeaders()
  {
    _streamRequestBuilder.clearHeaders();
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder clearCookies()
  {
    _streamRequestBuilder.clearCookies();
    return this;
  }

  public Map<String, String> getHeaders()
  {
    return _streamRequestBuilder.getHeaders();
  }

  public List<String> getCookies()
  {
    return _streamRequestBuilder.getCookies();
  }

  public String getHeader(final String name)
  {
    return _streamRequestBuilder.getHeader(name);
  }

  public List<String> getHeaderValues(final String name)
  {
    return _streamRequestBuilder.getHeaderValues(name);
  }

  public MultiPartMIMEStreamRequestBuilder unsafeSetHeader(final String name, final String value)
  {
    _streamRequestBuilder.unsafeSetHeader(name, value);
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder unsafeAddHeaderValue(final String name, final String value)
  {
    _streamRequestBuilder.unsafeAddHeaderValue(name, value);
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder unsafeSetHeaders(final Map<String, String> headers)
  {
    _streamRequestBuilder.unsafeSetHeaders(headers);
    return this;
  }

  public MultiPartMIMEStreamRequestBuilder unsafeOverwriteHeaders(final Map<String, String> headers)
  {
    _streamRequestBuilder.unsafeOverwriteHeaders(headers);
    return this;
  }
}