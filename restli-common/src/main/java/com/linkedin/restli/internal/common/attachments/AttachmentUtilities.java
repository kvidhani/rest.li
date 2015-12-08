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

package com.linkedin.restli.internal.common.attachments;


import com.linkedin.multipart.MultiPartMIMEDataSourceWriter;
import com.linkedin.multipart.MultiPartMIMEWriter;
import com.linkedin.r2.message.stream.entitystream.ByteStringWriter;
import com.linkedin.r2.message.stream.entitystream.WriteHandle;
import com.linkedin.restli.common.RestConstants;
import com.linkedin.restli.common.attachments.RestLiAttachmentDataSource;
import com.linkedin.restli.common.attachments.RestLiAttachmentReader;
import com.linkedin.restli.common.attachments.RestLiStreamingAttachments;
import java.util.Map;
import java.util.TreeMap;


/**
 * @autthor Karim Vidhani
 */
public class AttachmentUtilities
{
  public static final String RESTLI_MULTIPART_SUBTYPE = "related";

  public static MultiPartMIMEWriter createMultiPartMIMEWriter(final ByteStringWriter firstPartWriter,
                                                              final String firstPartContentType,
                                                              final RestLiStreamingAttachments streamingAttachments)
  {
    final MultiPartMIMEWriter.Builder multiPartMIMEWriterBuilder = new MultiPartMIMEWriter.Builder();
    multiPartMIMEWriterBuilder.appendDataSource(new MultiPartMIMEDataSourceWriter()
    {
      @Override
      public Map<String, String> dataSourceHeaders()
      {
        final Map<String, String> metaDataHeaders = new TreeMap<String, String>();
        metaDataHeaders.put(RestConstants.HEADER_CONTENT_TYPE, firstPartContentType);
        return metaDataHeaders;
      }

      @Override
      public void onInit(WriteHandle wh)
      {
        firstPartWriter.onInit(wh);
      }

      @Override
      public void onWritePossible()
      {
        firstPartWriter.onWritePossible();
      }

      @Override
      public void onAbort(Throwable e)
      {
        firstPartWriter.onAbort(e);
      }
    });

    //Now add all of the user specified attachments
    for (final Object streamingDataSource : streamingAttachments.getStreamingDataSources())
    {
      if (streamingDataSource instanceof RestLiAttachmentDataSource)
      {
        final RestLiAttachmentDataSource restLiAttachmentDataSource = (RestLiAttachmentDataSource) streamingDataSource;
        multiPartMIMEWriterBuilder.appendDataSource(new MultiPartMIMEDataSourceWriter()
        {
          @Override
          public Map<String, String> dataSourceHeaders()
          {
            final Map<String, String> dataSourceHeaders = new TreeMap<String, String>();
            dataSourceHeaders.put(RestConstants.HEADER_CONTENT_ID, restLiAttachmentDataSource.getAttachmentID());
            return dataSourceHeaders;
          }

          @Override
          public void onInit(WriteHandle wh)
          {
            restLiAttachmentDataSource.onInit(wh);
          }

          @Override
          public void onWritePossible()
          {
            restLiAttachmentDataSource.onWritePossible();
          }

          @Override
          public void onAbort(Throwable e)
          {
            restLiAttachmentDataSource.onAbort(e);
          }
        });
      }
      else
      {
        //Must be a RestLiAttachmentReader.
        final RestLiAttachmentReader restLiAttachmentReader = (RestLiAttachmentReader) streamingDataSource;
        multiPartMIMEWriterBuilder.appendMultiPartDataSource(restLiAttachmentReader.getMultiPartMIMEReader());
      }
    }

    return multiPartMIMEWriterBuilder.build();
  }
}