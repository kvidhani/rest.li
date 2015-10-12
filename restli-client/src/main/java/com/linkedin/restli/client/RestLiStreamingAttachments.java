package com.linkedin.restli.client;


import java.util.Collections;
import java.util.List;


/**
 * Created by kvidhani on 10/6/15.
 */
public class RestLiStreamingAttachments
{
  private final List<RestLiStreamingDataSource> _streamingDataSources;

  //todo - Builder to take in a variety of data sources include single part rest.li reader, restli reader and custom data sources
  public RestLiStreamingAttachments(final List<RestLiStreamingDataSource> streamingDataSources) {
    _streamingDataSources = streamingDataSources;
  }

  public RestLiStreamingAttachments(final RestLiStreamingDataSource streamingDataSource) {
    this(Collections.singletonList(streamingDataSource));
  }

  //package private
  List<RestLiStreamingDataSource> getStreamingDataSources()
  {
    return _streamingDataSources;
  }
}
