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


import com.linkedin.data.ByteString;
import com.linkedin.multipart.MultiPartMIMEReader;
import com.linkedin.multipart.MultiPartMIMEReaderCallback;
import com.linkedin.multipart.SinglePartMIMEReaderCallback;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.message.stream.entitystream.WriteHandle;
import com.linkedin.restli.common.RestConstants;


/**
 * Allows users to asynchronously walk through all attachments from an incoming request on the server side, or an
 * incoming response on the client side.
 *
 * @author Karim Vidhani
 */
public final class RestLiAttachmentReader
{
  private final MultiPartMIMEReader _multiPartMIMEReader;

  /**
   * Constructs a RestLiAttachmentReader by wrapping a {@link com.linkedin.multipart.MultiPartMIMEReader}.
   *
   * NOTE: This should not be instantiated directly by consumers of rest.li.
   *
   * @param multiPartMIMEReader the {@link com.linkedin.multipart.MultiPartMIMEReader} to wrap.
   */
  public RestLiAttachmentReader(final MultiPartMIMEReader multiPartMIMEReader)
  {
    _multiPartMIMEReader = multiPartMIMEReader;
  }

  /**
   * Determines if there are any more attachments to read. If the last attachment is in the process of being read,
   * this will return false.
   *
   * @return true if there are more attachments to read, or false if all attachments have been consumed.
   */
  public boolean haveAllAttachmentsFinished()
  {
    return _multiPartMIMEReader.haveAllPartsFinished();
  }

  /**
   * Reads through and abandons the current new attachment and additionally all remaining attachments. This API can ONLY be used after
   * registration via {@link RestLiAttachmentReader#registerAttachmentReaderCallback(com.linkedin.restli.common.attachments.RestLiAttachmentReaderCallback)}
   * and after an invocation on
   * {@link RestLiAttachmentReaderCallback#onNewAttachment(com.linkedin.restli.common.attachments.RestLiAttachmentReader.SingleRestLiAttachmentReader))}.
   *
   * If this is called after registration and before an invocation on
   * {@link RestLiAttachmentReaderCallback#onNewAttachment(com.linkedin.restli.common.attachments.RestLiAttachmentReader.SingleRestLiAttachmentReader))},
   * then a {@link com.linkedin.multipart.exceptions.StreamBusyException} will be thrown.
   *
   * The goal is for clients to at least see the first attachment before deciding to abandon all attachments.
   *
   * As described, a valid {@link com.linkedin.restli.common.attachments.RestLiAttachmentReaderCallback} is required to use this API.
   * Failure to do so will result in a {@link com.linkedin.multipart.exceptions.MultiPartReaderNotInitializedException}.
   *
   * This can ONLY be called if there is no attachment being actively read, meaning that
   * the current {@link com.linkedin.restli.common.attachments.RestLiAttachmentReader.SingleRestLiAttachmentReader}
   * has not been initialized with a {@link SingleRestLiAttachmentReaderCallback}.
   * If this is violated a {@link com.linkedin.multipart.exceptions.StreamBusyException} will be thrown.
   *
   * Once all attachments have finished being abandoned, a call will be made to {@link MultiPartMIMEReaderCallback#onAbandoned()}.
   *
   * If the stream is finished, subsequent calls will throw {@link com.linkedin.multipart.exceptions.MultiPartReaderFinishedException}.
   *
   * Since this is async and we do not allow request queueing, repetitive calls will result in
   * {@link com.linkedin.multipart.exceptions.StreamBusyException}.
   */
  public void abandonAllAttachments()
  {
    _multiPartMIMEReader.abandonAllParts();
  }

  /**
   * Returns the underlying {@link com.linkedin.multipart.MultiPartMIMEReader}.
   *
   * NOTE: This should not be used directly by consumers of rest.li.
   * @return
   */
  public MultiPartMIMEReader getMultiPartMIMEReader()
  {
    return _multiPartMIMEReader;
  }

  /**
   * Register to read using this RestLiAttachmentReader. This can ONLY be called if there is no attachment being actively
   * read meaning that the current {@link com.linkedin.restli.common.attachments.RestLiAttachmentReader.SingleRestLiAttachmentReader}
   * has not had a callback registered with it. Violation of this will throw a {@link com.linkedin.multipart.exceptions.StreamBusyException}.
   *
   * This can even be set if no attachments in the stream have actually been consumed, i.e after the very first invocation of
   * {@link RestLiAttachmentReaderCallback#onNewAttachment(com.linkedin.restli.common.attachments.RestLiAttachmentReader.SingleRestLiAttachmentReader))}.
   *
   * @param restLiAttachmentReaderCallback the callback to register with.
   */
  public void registerAttachmentReaderCallback(final RestLiAttachmentReaderCallback restLiAttachmentReaderCallback)
  {
    _multiPartMIMEReader.registerReaderCallback(new MultiPartMIMEReaderCallback()
    {
      @Override
      public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
      {
        //If there is no Content-ID in the response then we bail early
        final String contentID = singlePartMIMEReader.dataSourceHeaders().get(RestConstants.HEADER_CONTENT_ID);
        if (contentID == null)
        {
          onStreamError(new RemoteInvocationException("Illegally formed multipart mime envelope. RestLi attachment"
              + " is missing the ContentID!"));
        }
        restLiAttachmentReaderCallback.onNewAttachment(new SingleRestLiAttachmentReader(singlePartMIMEReader, contentID));
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

  /**
   * Allows users to asynchronously walk through all the data in an individual attachment. Instances of this
   * can only be constructed by a {@link com.linkedin.restli.common.attachments.RestLiAttachmentReader}.
   */
  public final class SingleRestLiAttachmentReader implements RestLiAttachmentDataSource
  {
    private final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    private final String _attachmentID;

    private SingleRestLiAttachmentReader(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader,
                                         final String attachmentID)
    {
      _singlePartMIMEReader = singlePartMIMEReader;
      _attachmentID = attachmentID;
    }

    /**
     * Denotes the unique identifier for this attachment.
     *
     * @return the {@link java.lang.String} representing this attachment.
     */
    @Override
    public String getAttachmentID()
    {
      return _attachmentID;
    }

    /**
     * Reads bytes from this attachment and notifies the registered callback on
     * {@link SingleRestLiAttachmentReaderCallback#onAttachmentDataAvailable(com.linkedin.data.ByteString)}.
     *
     * Usage of this API requires registration using a {@link SingleRestLiAttachmentReaderCallback}.
     * Failure to do so will throw a {@link com.linkedin.multipart.exceptions.SinglePartNotInitializedException}.
     *
     * If this attachment is fully consumed, meaning {@link SingleRestLiAttachmentReaderCallback#onFinished()}
     * has been called, then any subsequent calls to requestAttachmentData() will throw {@link com.linkedin.multipart.exceptions.SinglePartFinishedException}.
     *
     * Since this is async and request queueing is not allowed, repetitive calls will result in
     * {@link com.linkedin.multipart.exceptions.StreamBusyException}.
     *
     * If this reader is done, either through an error or a proper finish. Calls to requestAttachmentData() will throw
     * {@link com.linkedin.multipart.exceptions.SinglePartFinishedException}.
     */
    public void requestAttachmentData()
    {
      _singlePartMIMEReader.requestPartData();
    }

    /**
     * Abandons all bytes from this attachment and then notifies the registered callback (if present) on
     * {@link SingleRestLiAttachmentReaderCallback#onAbandoned()}.
     *
     * Usage of this API does NOT require registration using a {@link SingleRestLiAttachmentReaderCallback}.
     * If there is no callback registration then there is no notification provided upon completion of abandoning
     * this attachment.
     *
     * If this attachment is fully consumed, meaning {@link SingleRestLiAttachmentReaderCallback#onFinished()}
     * has been called, then any subsequent calls to abandonPart() will throw {@link com.linkedin.multipart.exceptions.SinglePartFinishedException}.
     *
     * Since this is async and request queueing is not allowed, repetitive calls will result in
     * {@link com.linkedin.multipart.exceptions.StreamBusyException}.
     *
     * If this reader is done, either through an error or a proper finish. Calls to abandonAttachment() will throw
     * {@link com.linkedin.multipart.exceptions.SinglePartFinishedException}.
     */
    public void abandonAttachment()
    {
      _singlePartMIMEReader.abandonPart();
    }

    /**
     * This call registers a callback and commits to reading this attachment. This can only happen once per life of each
     * SinglePartRestLiAttachmentReader. Subsequent attempts to modify this will throw
     * {@link com.linkedin.multipart.exceptions.SinglePartBindException}.
     *
     * @param callback the callback to be invoked on in order to read attachment data.
     */
    public void registerCallback(final SingleRestLiAttachmentReaderCallback callback)
    {
      _singlePartMIMEReader.registerReaderCallback(new SinglePartMIMEReaderCallback()
      {
        @Override
        public void onPartDataAvailable(ByteString partData)
        {
          callback.onAttachmentDataAvailable(partData);
        }

        @Override
        public void onFinished()
        {
          callback.onFinished();
        }

        @Override
        public void onAbandoned()
        {
          callback.onAbandoned();
        }

        @Override
        public void onStreamError(Throwable throwable)
        {
          callback.onAttachmentError(throwable);
        }
      });
    }

    //Implementation for Writer (for chaining).
    //NOTE: The following should NOT be used be external clients. These APIs are implementation details.
    @Override
    public void onInit(WriteHandle wh)
    {
      _singlePartMIMEReader.onInit(wh);
    }

    @Override
    public void onWritePossible()
    {
      _singlePartMIMEReader.onWritePossible();
    }

    @Override
    public void onAbort(Throwable e)
    {
      _singlePartMIMEReader.onAbort(e);
    }
  }
}