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

package com.linkedin.restli.examples.greetings.server;


import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.streaming.ByteStringWriter;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.attachments.RestLiAttachmentDataSource;
import com.linkedin.restli.common.attachments.RestLiAttachmentReader;
import com.linkedin.restli.common.attachments.RestLiAttachmentReaderCallback;
import com.linkedin.restli.common.attachments.RestLiStreamingAttachments;
import com.linkedin.restli.common.attachments.SingleRestLiAttachmentReaderCallback;
import com.linkedin.restli.examples.greetings.api.Greeting;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.CallbackParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceAsyncTemplate;

import java.io.ByteArrayOutputStream;


/**
 * @author Karim Vidhani
 */
@RestLiCollection(name = "streamingGreetings", namespace = "com.linkedin.restli.examples.greetings.streaming")
public class StreamingGreetings extends CollectionResourceAsyncTemplate<Long, Greeting>
{
  private static byte[] greetingBytes = "BeginningBytes".getBytes();

  public StreamingGreetings()
  {
  }

  @Override
  public void get(Long key, @CallbackParam Callback<Greeting> callback)
  {
    if(getContext().responseAttachmentsSupported())
    {
      final GreetingWriter greetingWriter = new GreetingWriter(ByteString.copy(greetingBytes));
      final RestLiStreamingAttachments streamingAttachments =
          new RestLiStreamingAttachments.Builder().appendDataSource(greetingWriter).build();
      getContext().setResponseAttachments(streamingAttachments);
      callback.onSuccess(new Greeting().setMessage("Your greeting has an attachment since you were kind and "
                                                   + "decided you wanted to read it!").setId(key));
    }
    callback.onError(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "You must be able to receive attachments!"));
  }

  @Override
  public void create(Greeting entity, @CallbackParam Callback<CreateResponse> callback)
  {
    if (getContext().requestAttachmentsPresent())
    {
      final RestLiAttachmentReader restLiAttachmentReader = getContext().getRestLiAttachmentReader();
      restLiAttachmentReader.registerAttachmentReaderCallback(new GreetingBlobReader(callback));
      return;
    }
    callback.onError(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "You must supply some attachments!"));
  }

  //For writing the response attachment
  private static class GreetingWriter extends ByteStringWriter implements RestLiAttachmentDataSource
  {
    private GreetingWriter(final ByteString content)
    {
      super(content);
    }

    @Override
    public String getAttachmentID()
    {
      return "12345";
    }
  }

  //For reading in the request attachment
  private static class GreetingBlobReader implements RestLiAttachmentReaderCallback
  {
    private final Callback<CreateResponse> _createResponseCallback;

    private GreetingBlobReader(final Callback<CreateResponse> createResponseCallback)
    {
      _createResponseCallback = createResponseCallback;
    }

    @Override
    public void onNewPart(RestLiAttachmentReader.SingleRestLiAttachmentReader singleRestLiAttachmentReader)
    {
      final SingleGreetingBlobReader singleGreetingBlobReader = new SingleGreetingBlobReader(this,
                                                                                             singleRestLiAttachmentReader);
      singleRestLiAttachmentReader.registerCallback(singleGreetingBlobReader);
      singleRestLiAttachmentReader.requestAttachmentData();
    }

    @Override
    public void onFinished()
    {
      _createResponseCallback.onSuccess(new CreateResponse(150));
    }

    @Override
    public void onAbandoned()
    {
      _createResponseCallback.onError(new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR));
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      _createResponseCallback.onError(new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR));
    }
  }

  private static class SingleGreetingBlobReader implements SingleRestLiAttachmentReaderCallback
  {
    private final RestLiAttachmentReaderCallback _topLevelCallback;
    private final RestLiAttachmentReader.SingleRestLiAttachmentReader _singleRestLiAttachmentReader;
    private final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();

    public SingleGreetingBlobReader(RestLiAttachmentReaderCallback topLevelCallback,
                                    RestLiAttachmentReader.SingleRestLiAttachmentReader singleRestLiAttachmentReader)
    {
      _topLevelCallback = topLevelCallback;
      _singleRestLiAttachmentReader = singleRestLiAttachmentReader;
    }

    @Override
    public void onAttachmentDataAvailable(ByteString attachmentData)
    {
      try
      {
        _byteArrayOutputStream.write(attachmentData.copyBytes());
        _singleRestLiAttachmentReader.requestAttachmentData();
      }
      catch (Exception exception)
      {
        _topLevelCallback.onStreamError(new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR));
      }
    }

    @Override
    public void onFinished()
    {
      greetingBytes = _byteArrayOutputStream.toByteArray();
    }

    @Override
    public void onAbandoned()
    {
      _topLevelCallback.onStreamError(new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR));
    }

    @Override
    public void onAttachmentError(Throwable throwable)
    {
      //No need to do anything since the top level callback will get invoked with an error anyway
    }
  }
}
