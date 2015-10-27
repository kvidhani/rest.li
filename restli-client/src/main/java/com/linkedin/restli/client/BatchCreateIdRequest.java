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

package com.linkedin.restli.client;


import com.linkedin.data.template.RecordTemplate;
import com.linkedin.restli.common.BatchCreateIdResponse;
import com.linkedin.restli.common.CollectionRequest;
import com.linkedin.restli.common.ResourceMethod;
import com.linkedin.restli.common.ResourceSpec;
import com.linkedin.restli.common.attachments.RestLiStreamingAttachments;
import com.linkedin.restli.internal.client.BatchCreateIdDecoder;

import java.util.Map;


/**
 * Batch Create Request that can return strongly-typed keys as first-class citizens.
 * Used by *RequestBuilder named Builders.
 *
 * @author Moira Tagle
 */
public class BatchCreateIdRequest<K,T extends RecordTemplate> extends Request<BatchCreateIdResponse<K>>
{
  BatchCreateIdRequest(Map<String, String> headers,
                       BatchCreateIdDecoder<K> decoder,
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
