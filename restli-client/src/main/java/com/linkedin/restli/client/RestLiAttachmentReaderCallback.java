package com.linkedin.restli.client;


/**
 * Created by kvidhani on 10/13/15.
 */
public interface RestLiAttachmentReaderCallback
{
  public void onNewPart(RestLiAttachmentReader.SinglePartRestLiAttachmentReader singlePartRestLiAttachmentReader);

  public void onFinished();

  public void onAbandoned();

  public void onStreamError(Throwable throwable);
}
