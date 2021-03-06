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
package com.linkedin.r2.filter.message.rest;

/**
 * A filter that processes {@link com.linkedin.r2.message.rest.RestRequest}s and
 * {@link com.linkedin.r2.message.rest.RestResponse}s.
 *
 * This will be ignored if directly added to filter chain. If we intend to use it,
 * adapt it with {@link com.linkedin.r2.filter.message.rest.StreamFilterAdapters#adaptRestFilter(com.linkedin.r2.filter.Filter)}
 *
 * @author Chris Pettitt
 */
public interface RestFilter extends RestRequestFilter, RestResponseFilter
{

}
