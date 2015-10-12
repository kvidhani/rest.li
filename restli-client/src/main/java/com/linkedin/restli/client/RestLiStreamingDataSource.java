package com.linkedin.restli.client;


import com.linkedin.r2.message.streaming.Writer;


/**
 * Created by kvidhani on 10/8/15.
 */
public interface RestLiStreamingDataSource extends Writer
{
  public String getAttachmentID();
}
