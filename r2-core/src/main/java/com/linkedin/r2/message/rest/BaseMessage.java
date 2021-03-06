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
package com.linkedin.r2.message.rest;


import com.linkedin.util.ArgumentUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
/* package private */ abstract class BaseMessage implements MessageHeaders
{
  private final Map<String, String> _headers;

  private final List<String> _cookies;

  protected BaseMessage(Map<String, String> headers, List<String> cookies)
  {
    ArgumentUtil.notNull(headers, "headers");
    ArgumentUtil.notNull(cookies, "cookies");
    _headers = headers;
    _cookies = cookies;
  }

  @Override
  public String getHeader(String name)
  {
    return _headers.get(name);
  }

  @Override
  public List<String> getHeaderValues(String name)
  {
    final String headerVal = getHeader(name);
    if (headerVal == null)
    {
      return null;
    }
    return RestUtil.getHeaderValues(headerVal);
  }

  @Override
  public Map<String, String> getHeaders()
  {
    return Collections.unmodifiableMap(_headers);
  }

  @Override
  public List<String> getCookies()
  {
    return Collections.unmodifiableList(_cookies);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o)
    {
      return true;
    }
    if (!(o instanceof BaseMessage))
    {
      return false;
    }

    BaseMessage that = (BaseMessage) o;
    return _headers.equals(that._headers) && _cookies.equals(that._cookies);
  }

  @Override
  public int hashCode()
  {
    int result = _headers.hashCode();
    return 31 * result + _cookies.hashCode();
  }
}
