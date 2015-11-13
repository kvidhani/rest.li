/*
   Copyright (c) 2014 LinkedIn Corp.

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

package com.linkedin.restli.server.filter;


/**
 * A filter that processes incoming requests to RestLi resources.
 *
 * @author nshankar
 */
public interface RequestFilter
{
  /**
   * Request filter method to be invoked before the execution of the resource.
   *
   * @param requestContext    Reference to {@link FilterRequestContext}.
   * @param nextRequestFilter The next filter in the chain.  Concrete implementations should invoke {@link
   *                          NextRequestFilter#onRequest to continue the filter chain.
   */
  void onRequest(final FilterRequestContext requestContext, final NextRequestFilter nextRequestFilter);
}
