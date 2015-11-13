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

package com.linkedin.restli.common.attachments;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Represents an ordered list of attachments to be sent in a request or sent back in a response.
 *
 * @author Karim Vidhani
 */
public class RestLiStreamingAttachments
{
  private final List<Object> _allDataSources;

  /**
   * Builder to create an instance of RestLiStreamingAttachments.
   */
  public static class Builder
  {
    final List<Object> _allDataSources;

    /**
     * Create a RestLiStreamingAttachments Builder.
     *
     * @return the builder to continue building.
     */
    public Builder()
    {
      _allDataSources = new ArrayList<Object>();
    }

    /**
     * Append a {@link RestLiAttachmentDataSource} to be placed as an attachment.
     *
     * @param dataSource the data source to be added.
     * @return the builder to continue building.
     */
    public Builder appendDataSource(final RestLiAttachmentDataSource dataSource)
    {
      _allDataSources.add(dataSource);
      return this;
    }

    /**
     * Append a {@link RestLiAttachmentReader} to be used as a data source
     * within the newly constructed attachment list. All the individual attachments remaining from the
     * {@link RestLiAttachmentReader} will chained and placed as attachments in the new
     * attachment list.
     *
     * @param attachmentReader
     * @return the builder to continue building.
     */
    public Builder appendRestLiAttachmentReader(final RestLiAttachmentReader attachmentReader)
    {
      _allDataSources.add(attachmentReader);
      return this;
    }

    /**
     * Append multiple {@link RestLiAttachmentDataSource}s to be placed as attachments.
     *
     * @param dataSources the data sources to be added.
     * @return the builder to continue building.
     */
    public Builder appendDataSources(final List<RestLiAttachmentDataSource> dataSources)
    {
      for (final RestLiAttachmentDataSource dataSource : dataSources)
      {
        appendDataSource(dataSource);
      }
      return this;
    }

    /**
     * Append multiple {@link RestLiAttachmentReader}s to be used as data sources within
     * the newly constructed attachment list. All the individual attachments remaining from each of the
     * {@link RestLiAttachmentReader}s will chained and placed as attachments in the new
     * attachment list.
     *
     * @param attachmentReaders
     * @return the builder to continue building.
     */
    public Builder appendRestLiAttachmentReaders(final List<RestLiAttachmentReader> attachmentReaders)
    {
      for (RestLiAttachmentReader attachmentReader : attachmentReaders)
      {
        appendRestLiAttachmentReader(attachmentReader);
      }
      return this;
    }

    /**
     * Construct and return the newly formed {@link RestLiStreamingAttachments}.
     * @return the fully constructed {@link RestLiStreamingAttachments}.
     */
    public RestLiStreamingAttachments build()
    {
      return new RestLiStreamingAttachments(this);
    }
  }

  private RestLiStreamingAttachments(final RestLiStreamingAttachments.Builder builder)
  {
    _allDataSources = builder._allDataSources;
  }

  public List<Object> getStreamingDataSources()
  {
    return Collections.unmodifiableList(_allDataSources);
  }
}
