package com.linkedin.restli.client;


import com.linkedin.data.ByteString;
import com.linkedin.multipart.MultiPartMIMEReader;
import com.linkedin.multipart.MultiPartMIMEReaderCallback;
import com.linkedin.multipart.SinglePartMIMEReaderCallback;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.common.RestConstants;


/**
 * Created by kvidhani on 10/12/15.
 */
public final class RestLiAttachmentReader
{
  private final MultiPartMIMEReader _multiPartMIMEReader;

  public RestLiAttachmentReader(final MultiPartMIMEReader multiPartMIMEReader)
  {
    _multiPartMIMEReader = multiPartMIMEReader;
  }

  public boolean haveAllPartsFinished()
  {
    return _multiPartMIMEReader.haveAllPartsFinished();
  }

  public void abandonAllParts()
  {
    //Todo catch and wrap exceptions
    _multiPartMIMEReader.abandonAllParts();
  }

  public void registerAttachmentReaderCallback(final RestLiAttachmentReaderCallback restLiAttachmentReaderCallback)
  {
    _multiPartMIMEReader.registerReaderCallback(new MultiPartMIMEReaderCallback()
    {
      @Override
      public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
      {
        //If there is no Content-ID in the response then we bail early
        final String contentID = singleParMIMEReader.getHeaders().get(RestConstants.HEADER_CONTENT_ID);
        if (contentID == null)
        {
          onStreamError(new RemoteInvocationException("Illegally formed multipart mime envelope. Restli attachment"
              + " is missing the ContentID!"));
        }
        restLiAttachmentReaderCallback.onNewPart(new SinglePartRestLiAttachmentReader(singleParMIMEReader, contentID));
      }

      @Override
      public void onFinished()
      {
        restLiAttachmentReaderCallback.onFinished();
      }

      @Override
      public void onAbandoned()
      {
        restLiAttachmentReaderCallback.onAbandoned();
      }

      @Override
      public void onStreamError(Throwable throwable)
      {
        restLiAttachmentReaderCallback.onStreamError(throwable);
      }
    });
  }


  public final class SinglePartRestLiAttachmentReader
  {
    private final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    private final String _contentID;

    private SinglePartRestLiAttachmentReader(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader,
        final String contentID)
    {
      _singlePartMIMEReader = singlePartMIMEReader;
      _contentID = contentID;
    }

    public String getContentID()
    {
      return _contentID;
    }

    public void abandonAttachment()
    {
      //Todo wrap exceptions
      _singlePartMIMEReader.abandonPart();
    }

    public void registerCallback(final SinglePartRestLiAttachmentReaderCallback singlePartRestLiAttachmentReaderCallback)
    {
      _singlePartMIMEReader.registerReaderCallback(new SinglePartMIMEReaderCallback()
      {
        @Override
        public void onPartDataAvailable(ByteString partData)
        {
          singlePartRestLiAttachmentReaderCallback.onAttachmentDataAvailable(partData);
        }

        @Override
        public void onFinished()
        {
          singlePartRestLiAttachmentReaderCallback.onFinished();
        }

        @Override
        public void onAbandoned()
        {
          singlePartRestLiAttachmentReaderCallback.onAbandoned();
        }

        @Override
        public void onStreamError(Throwable throwable)
        {
          //Todo perhaps wrap?
          singlePartRestLiAttachmentReaderCallback.onAttachmentError(throwable);
        }
      });
    }

    public void requestAttachmentData()
    {
      _singlePartMIMEReader.requestPartData();
    }
  }

}
