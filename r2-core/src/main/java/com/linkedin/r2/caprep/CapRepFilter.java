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
package com.linkedin.r2.caprep;


import com.linkedin.r2.caprep.db.DefaultMessageSerializer;
import com.linkedin.r2.caprep.db.DirectoryDbSink;
import com.linkedin.r2.caprep.db.DirectoryDbSource;
import com.linkedin.r2.filter.Filter;
import com.linkedin.r2.filter.NextFilter;
import com.linkedin.r2.filter.message.rest.StreamFilter;
import com.linkedin.r2.filter.message.rest.StreamFilterAdapters;
import com.linkedin.r2.message.RequestContext;

import java.io.IOException;
import java.util.Map;

import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.rest.StreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WARNING: This filter is not compatible with r2 streaming feature.
 * Use this filter would result in both request and response being fully cached in memory; do not use with
 * large requests/responses.
 *
 * @author Chris Pettitt
 * @version $Revision$
 */
public class CapRepFilter implements StreamFilter, CapRepAdmin
{
  private static final Logger _log = LoggerFactory.getLogger(CapRepFilter.class);

  private static final Filter PASS_THROUGH_FILTER = new PassThroughFilter();

  private final ReplaceableFilter _filter = new ReplaceableFilter(PASS_THROUGH_FILTER);

  @Override
  public void capture(String directory) throws IOException
  {
    _log.debug("Switching to capture mode. Directory: " + directory);
    _filter.setFilter(PASS_THROUGH_FILTER);
    try
    {
      _filter.setFilter(StreamFilterAdapters.adaptRestFilter(new CaptureFilter(new DirectoryDbSink(directory,
                                                              new DefaultMessageSerializer()))));
    }
    catch (IOException e)
    {
      _log.warn("Error switching to capture mode", e);
      throw e;
    }
    catch (RuntimeException e)
    {
      _log.warn("Error switching to capture mode", e);
      throw e;
    }
  }

  @Override
  public void replay(String directory) throws IOException
  {
    _log.debug("Switching to replay mode. Directory: " + directory);
    _filter.setFilter(PASS_THROUGH_FILTER);
    try
    {
      _filter.setFilter(StreamFilterAdapters.adaptRestFilter(new ReplayFilter(new DirectoryDbSource(directory,
                                                               new DefaultMessageSerializer()))));
    }
    catch (IOException e)
    {
      _log.warn("Error switching to replay mode", e);
      throw e;
    }
    catch (RuntimeException e)
    {
      _log.warn("Error switching to capture mode", e);
      throw e;
    }
  }

  @Override
  public void passThrough()
  {
    _log.debug("Switching to pass-through mode.");
    _filter.setFilter(PASS_THROUGH_FILTER);
  }

  @Override
  public String getMode()
  {
    return _filter.getFilter().getClass().getSimpleName();
  }

  @Override
  public void onRequest(StreamRequest req, RequestContext requestContext,
                            Map<String, String> wireAttrs,
                            NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    _filter.onRequest(req, requestContext, wireAttrs, nextFilter);
  }

  @Override
  public void onResponse(StreamResponse res, RequestContext requestContext,
                             Map<String, String> wireAttrs,
                             NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    _filter.onResponse(res, requestContext, wireAttrs, nextFilter);
  }

  @Override
  public void onError(Throwable ex, RequestContext requestContext,
                          Map<String, String> wireAttrs,
                          NextFilter<StreamRequest, StreamResponse> nextFilter)
  {
    _filter.onError(ex, requestContext, wireAttrs, nextFilter);
  }
}
