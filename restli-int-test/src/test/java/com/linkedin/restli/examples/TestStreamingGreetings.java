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

package com.linkedin.restli.examples;


import com.linkedin.data.ByteString;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.message.stream.entitystream.ByteStringWriter;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.CreateIdRequestBuilder;
import com.linkedin.restli.client.ProtocolVersionOption;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.client.RestliRequestOptionsBuilder;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.attachments.RestLiAttachmentDataSource;
import com.linkedin.restli.common.attachments.RestLiAttachmentReader;
import com.linkedin.restli.common.attachments.RestLiAttachmentReaderCallback;
import com.linkedin.restli.common.attachments.RestLiStreamingAttachments;
import com.linkedin.restli.common.attachments.SingleRestLiAttachmentReaderCallback;
import com.linkedin.restli.examples.greetings.api.Greeting;
import com.linkedin.restli.examples.greetings.streaming.StreamingGreetingsBuilders;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.test.util.RootBuilderWrapper;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Integration tests for rest.li attachment streaming.
 * //TODO - more tests and richer assertions
 *
 * @author Karim Vidhani
 */
public class TestStreamingGreetings extends RestLiIntegrationTest
{
  @BeforeClass
  public void initClass() throws Exception
  {
    super.init();
  }

  @AfterClass
  public void shutDown() throws Exception
  {
    super.shutdown();
  }

  @Test(dataProvider = com.linkedin.restli.internal.common.TestConstants.RESTLI_PROTOCOL_1_2_PREFIX
      + "requestBuilderDataProvider")
  public void simpleStreamTest(final RootBuilderWrapper<Long, Greeting> builders)
      throws RemoteInvocationException
  {
    //Perform a create to the server to store some bytes via an attachment.
    final byte[] clientSuppliedBytes = "ClientSupplied".getBytes();
    final GreetingWriter greetingWriter = new GreetingWriter(ByteString.copy(clientSuppliedBytes));

    final RootBuilderWrapper.MethodBuilderWrapper<Long, Greeting, EmptyRecord> methodBuilderWrapper = builders.create();

    final RestLiStreamingAttachments attachments = new RestLiStreamingAttachments.Builder().appendDataSource(greetingWriter).build();
    methodBuilderWrapper.streamingAttachments(attachments);

    final Greeting greeting = new Greeting().setMessage("A greeting with an attachment");
    if (methodBuilderWrapper.isRestLi2Builder())
    {
      final Object objBuilder = methodBuilderWrapper.getBuilder();
      @SuppressWarnings("unchecked")
      final CreateIdRequestBuilder<Long, Greeting> createIdRequestBuilder =
          (CreateIdRequestBuilder<Long, Greeting>) objBuilder;
      final CreateIdRequest<Long, Greeting> request = createIdRequestBuilder.input(greeting).build();
      try
      {
        getClient().sendRequest(request).getResponse();
      }
      catch (final RestLiResponseException responseException)
      {
        Assert.fail("We should not reach here!", responseException);
      }
    }
    else
    {
      final Request<EmptyRecord> request = methodBuilderWrapper.input(greeting).build();
      try
      {
        getClient().sendRequest(request).getResponse();
      }
      catch (final RestLiResponseException responseException)
      {
        Assert.fail("We should not reach here!", responseException);
      }
    }

    //Then perform a GET and verify the bytes are present
    try
    {
      final Response<Greeting> response = getClient().sendRequest(builders.get().id(1l).build()).getResponse();
      Assert.assertEquals(response.getEntity().getMessage(),
                          "Your greeting has an attachment since you were kind and decided you wanted to read it!");
      Assert.assertTrue(response.hasAttachments(), "We must have some response attachments");
      RestLiAttachmentReader attachmentReader = response.getAttachmentReader();
      final CountDownLatch latch = new CountDownLatch(1);
      final GreetingBlobReader greetingBlobReader = new GreetingBlobReader(latch);
      attachmentReader.registerAttachmentReaderCallback(greetingBlobReader);
      try
      {
        latch.await();
        Assert.assertEquals(greetingBlobReader.getAttachmentList().size(), 1);
        Assert.assertEquals(greetingBlobReader.getAttachmentList().get(0), ByteString.copy(clientSuppliedBytes));
      }
      catch (Exception exception)
      {
        Assert.fail();
      }
    }
    catch (final RestLiResponseException responseException)
    {
      Assert.fail("We should not reach here!", responseException);
    }
  }

  @DataProvider(name = com.linkedin.restli.internal.common.TestConstants.RESTLI_PROTOCOL_1_2_PREFIX
      + "requestBuilderDataProvider")
  private static Object[][] requestBuilderDataProvider()
  {
    final RestliRequestOptions defaultOptions =
        new RestliRequestOptionsBuilder().setProtocolVersionOption(ProtocolVersionOption.USE_LATEST_IF_AVAILABLE)
            .setAcceptResponseAttachments(true)
            .build();

    final RestliRequestOptions nextOptions =
        new RestliRequestOptionsBuilder().setProtocolVersionOption(ProtocolVersionOption.FORCE_USE_NEXT)
            .setAcceptResponseAttachments(true)
            .build();

    return new Object[][]
        {
            {
                new RootBuilderWrapper<Long, Greeting>(new StreamingGreetingsBuilders(defaultOptions))
            },
            {
                new RootBuilderWrapper<Long, Greeting>(new StreamingGreetingsBuilders(nextOptions))
            }
        };
  }

  //For writing the request attachment
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

  //For reading the response attachment
  private static class GreetingBlobReader implements RestLiAttachmentReaderCallback
  {
    private final CountDownLatch _countDownLatch;
    private List<ByteString> _attachmentsRead = new ArrayList<ByteString>();

    private GreetingBlobReader(CountDownLatch countDownLatch)
    {
      _countDownLatch = countDownLatch;
    }

    private void addAttachment(final ByteString attachment)
    {
      _attachmentsRead.add(attachment);
    }

    private List<ByteString> getAttachmentList()
    {
      return _attachmentsRead;
    }

    @Override
    public void onNewAttachment(RestLiAttachmentReader.SingleRestLiAttachmentReader singleRestLiAttachmentReader)
    {
      final SingleGreetingBlobReader singleGreetingBlobReader = new SingleGreetingBlobReader(this,
                                                                                             singleRestLiAttachmentReader);
      singleRestLiAttachmentReader.registerCallback(singleGreetingBlobReader);
      singleRestLiAttachmentReader.requestAttachmentData();
    }

    @Override
    public void onFinished()
    {
      _countDownLatch.countDown();
    }

    @Override
    public void onAbandoned()
    {
      Assert.fail();
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      Assert.fail();
    }
  }

  private static class SingleGreetingBlobReader implements SingleRestLiAttachmentReaderCallback
  {
    private final GreetingBlobReader _topLevelCallback;
    private final RestLiAttachmentReader.SingleRestLiAttachmentReader _singleRestLiAttachmentReader;
    private final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();

    public SingleGreetingBlobReader(GreetingBlobReader topLevelCallback,
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
      _topLevelCallback.addAttachment(ByteString.copy(_byteArrayOutputStream.toByteArray()));
    }

    @Override
    public void onAbandoned()
    {
      Assert.fail();
    }

    @Override
    public void onAttachmentError(Throwable throwable)
    {
      //No need to do anything since the top level callback will get invoked with an error anyway
    }
  }
}