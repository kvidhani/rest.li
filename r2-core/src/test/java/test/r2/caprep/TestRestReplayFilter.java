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
package test.r2.caprep;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.message.rest.Messages;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.message.rest.StreamException;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.r2.testutils.filter.CaptureLastCallFilter;
import com.linkedin.r2.testutils.filter.FilterUtil;

/**
 * @author Chris Pettitt
 * @version $Revision$
 */
public class TestRestReplayFilter extends AbstractReplayFilterTest
{
  @Test
  public void testReplayWithRestException()
  {
    final RestRequest req = request();
    final RestResponse res = new RestResponseBuilder().setStatus(RestStatus.NOT_FOUND).build();

    final CaptureLastCallFilter captureFilter = new CaptureLastCallFilter();
    final FilterChain fc = getFilterChain().addFirst(captureFilter);

    // Record a response for the request we will fire
    getDb().record(req, res);

    // We should be able to fire just the request - the response should be replayed from the
    // capture we set up above. The response should be a RestException since the status code is 404.
    FilterUtil.fireUntypedRequest(fc, Messages.toStreamRequest(req));

    Assert.assertTrue(captureFilter.getLastErr() instanceof StreamException);
    Messages.toRestResponse(((StreamException)captureFilter.getLastErr()).getResponse(), new Callback<RestResponse>()
    {
      @Override
      public void onError(Throwable e)
      {
        throw new RuntimeException(e);
      }

      @Override
      public void onSuccess(RestResponse result)
      {
        Assert.assertEquals(res, result);
      }
    });
  }

  @Override
  protected RestRequest request()
  {
    return FilterUtil.simpleRestRequest();
  }

  @Override
  protected RestResponse response()
  {
    return FilterUtil.simpleRestResponse();
  }
}
