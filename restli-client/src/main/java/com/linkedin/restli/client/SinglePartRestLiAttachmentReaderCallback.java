package com.linkedin.restli.client;


import com.linkedin.data.ByteString;


/**
 * Created by kvidhani on 10/13/15.
 */
//todo java docs everywhere
public interface SinglePartRestLiAttachmentReaderCallback
{
  public void onAttachmentDataAvailable(ByteString attachmentData);

  public void onFinished();

  public void onAbandoned();

  public void onAttachmentError(Throwable throwable);
}
