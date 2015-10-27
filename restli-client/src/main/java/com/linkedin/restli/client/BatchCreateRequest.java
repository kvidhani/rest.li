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

package com.linkedin.restli.client;


import com.linkedin.data.template.RecordTemplate;
import com.linkedin.restli.common.CollectionRequest;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.CreateStatus;
import com.linkedin.restli.common.ResourceMethod;
import com.linkedin.restli.common.ResourceSpec;
import com.linkedin.restli.common.attachments.RestLiStreamingAttachments;
import com.linkedin.restli.internal.client.BatchCreateDecoder;

import java.util.Map;


/**
 * This class has been deprecated. Please use {@link BatchCreateIdRequest} instead.
 *
 * @author Josh Walker
 */
public class BatchCreateRequest<T extends RecordTemplate> extends Request<CollectionResponse<CreateStatus>>
{
  BatchCreateRequest(Map<String, String> headers,
                     BatchCreateDecoder<?> decoder,
                     CollectionRequest<T> input,
                     ResourceSpec resourceSpec,
                     Map<String, Object> queryParams,
                     String baseUriTemplate,
                     Map<String, Object> pathKeys,
                     RestliRequestOptions requestOptions,
                     RestLiStreamingAttachments streamingAttachments)
  {
    super(ResourceMethod.BATCH_CREATE,
          input,
          headers,
          decoder,
          resourceSpec,
          queryParams,
          null,
          baseUriTemplate,
          pathKeys,
          requestOptions,
          streamingAttachments);
  }
}
