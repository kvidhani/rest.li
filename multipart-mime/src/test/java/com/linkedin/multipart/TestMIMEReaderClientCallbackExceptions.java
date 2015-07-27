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

package com.linkedin.multipart;


import com.linkedin.data.ByteString;
import com.linkedin.multipart.exceptions.PartFinishedException;
import com.linkedin.multipart.exceptions.ReaderFinishedException;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.ReadHandle;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.*;

import static com.linkedin.multipart.DataSources.*;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for making sure that the {@link com.linkedin.multipart.MultiPartMIMEReader} is resilient in the face of
 * exceptions thrown by invoking client callbacks.
 *
 * @author Karim Vidhani
 */
public class TestMIMEReaderClientCallbackExceptions extends AbstractMIMEUnitTest
{
  MultiPartMIMEReader _reader;
  MultiPartMIMEExceptionReaderCallbackImpl _currentMultiPartMIMEReaderCallback;

  @BeforeMethod
  public void setup()
  {
    SinglePartMIMEExceptionReaderCallbackImpl.resetAllFlags();
    MultiPartMIMEExceptionReaderCallbackImpl.resetAllFlags();
  }

  //MultiPartMIMEReader callback invocations throwing exceptions:
  //These tests all verify the resilience of the multipart mime reader when multipart mime reader client callbacks throw runtime exceptions
  @DataProvider(name = "allTypesOfBodiesDataSource")
  public Object[][] allTypesOfBodiesDataSource() throws Exception
  {
    final List<MimeBodyPart> bodyPartList = new ArrayList<MimeBodyPart>();
    bodyPartList.add(_smallDataSource);
    bodyPartList.add(_largeDataSource);
    bodyPartList.add(_headerLessBody);
    bodyPartList.add(_bodyLessBody);
    bodyPartList.add(_bytesBody);
    bodyPartList.add(_purelyEmptyBody);

    return new Object[][]{{1, bodyPartList}, {R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList}};
  }

  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testMultiPartMIMEReaderCallbackExceptionOnNewPart(final int chunkSize,
      final List<MimeBodyPart> bodyPartList) throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList)
    {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());

    MultiPartMIMEExceptionReaderCallbackImpl.throwOnNewPart = true;
    CountDownLatch countDownLatch =
        executeRequestPartialReadWithException(requestPayload, chunkSize, multiPartMimeBody.getContentType());

    countDownLatch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._streamError instanceof NullPointerException);
    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 0);

    try
    {
      _currentMultiPartMIMEReaderCallback._reader.abandonAllParts();
      Assert.fail();
    }
    catch (ReaderFinishedException readerFinishedException)
    {
      //pass
    }
  }

  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testMultiPartMIMEReaderCallbackExceptionOnFinished(final int chunkSize,
      final List<MimeBodyPart> bodyPartList) throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList)
    {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());

    MultiPartMIMEExceptionReaderCallbackImpl.throwOnFinished = true;
    CountDownLatch countDownLatch =
        executeRequestPartialReadWithException(requestPayload, chunkSize, multiPartMimeBody.getContentType());

    countDownLatch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._streamError instanceof NullPointerException);

    //Verify this are unusable.
    try
    {
      _currentMultiPartMIMEReaderCallback._reader.abandonAllParts();
      Assert.fail();
    }
    catch (ReaderFinishedException readerFinishedException)
    {
      //pass
    }

    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 6);
    //None of the single part callbacks should have recieved the error since they were all done before the top
    //callback threw
    for (int i = 0; i < _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(); i++)
    {
      Assert.assertNull(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(i)._streamError);
      //Verify this are unusable.
      try
      {
        _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(i)._singlePartMIMEReader
            .requestPartData();
        Assert.fail();
      }
      catch (PartFinishedException partFinishedException)
      {
        //pass
      }
    }
  }

  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testMultiPartMIMEReaderCallbackExceptionOnAbandoned(final int chunkSize,
      final List<MimeBodyPart> bodyPartList) throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList)
    {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());

    MultiPartMIMEExceptionReaderCallbackImpl.throwOnAbandoned = true;
    CountDownLatch countDownLatch =
        executeRequestPartialReadWithException(requestPayload, chunkSize, multiPartMimeBody.getContentType());

    countDownLatch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._streamError instanceof NullPointerException);
    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 0);

    //Verify this are unusable.
    try
    {
      _currentMultiPartMIMEReaderCallback._reader.abandonAllParts();
      Assert.fail();
    }
    catch (ReaderFinishedException readerFinishedException)
    {
      //pass
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  //SinglePartMIMEReader callback invocations throwing exceptions:
  //These tests all verify the resilience of the single part mime reader when single part mime reader client callbacks throw runtime exceptions
  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testSinglePartMIMEReaderCallbackExceptionOnPartDataAvailable(final int chunkSize,
      final List<MimeBodyPart> bodyPartList) throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList)
    {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());

    SinglePartMIMEExceptionReaderCallbackImpl.throwOnPartDataAvailable = true;
    CountDownLatch countDownLatch =
        executeRequestPartialReadWithException(requestPayload, chunkSize, multiPartMimeBody.getContentType());

    countDownLatch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._streamError instanceof NullPointerException);
    //Verify this are unusable.
    try
    {
      _currentMultiPartMIMEReaderCallback._reader.abandonAllParts();
      Assert.fail();
    }
    catch (ReaderFinishedException readerFinishedException)
    {
      //pass
    }

    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 1);
    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks
        .get(0)._streamError instanceof NullPointerException);
    try
    {
      _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._singlePartMIMEReader.requestPartData();
      Assert.fail();
    }
    catch (PartFinishedException partFinishedException)
    {
      //pass
    }
  }

  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testSinglePartMIMEReaderCallbackExceptionOnFinished(final int chunkSize,
      final List<MimeBodyPart> bodyPartList) throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList)
    {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());

    SinglePartMIMEExceptionReaderCallbackImpl.throwOnFinished = true;
    CountDownLatch countDownLatch =
        executeRequestPartialReadWithException(requestPayload, chunkSize, multiPartMimeBody.getContentType());

    countDownLatch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._streamError instanceof NullPointerException);
    //Verify this are unusable.
    try
    {
      _currentMultiPartMIMEReaderCallback._reader.abandonAllParts();
      Assert.fail();
    }
    catch (ReaderFinishedException readerFinishedException)
    {
      //pass
    }

    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 1);
    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks
        .get(0)._streamError instanceof NullPointerException);
    try
    {
      _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._singlePartMIMEReader.requestPartData();
      Assert.fail();
    }
    catch (PartFinishedException partFinishedException)
    {
      //pass
    }
  }

  @Test(dataProvider = "allTypesOfBodiesDataSource")
  public void testSinglePartMIMEReaderCallbackExceptionOnAbandoned(final int chunkSize,
      final List<MimeBodyPart> bodyPartList) throws Exception
  {
    MimeMultipart multiPartMimeBody = new MimeMultipart();

    //Add your body parts
    for (final MimeBodyPart bodyPart : bodyPartList)
    {
      multiPartMimeBody.addBodyPart(bodyPart);
    }

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    multiPartMimeBody.writeTo(byteArrayOutputStream);
    final ByteString requestPayload = ByteString.copy(byteArrayOutputStream.toByteArray());

    SinglePartMIMEExceptionReaderCallbackImpl.throwOnAbandoned = true;
    CountDownLatch countDownLatch =
        executeRequestPartialReadWithException(requestPayload, chunkSize, multiPartMimeBody.getContentType());

    countDownLatch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._streamError instanceof NullPointerException);
    //Verify these are unusable.
    try
    {
      _currentMultiPartMIMEReaderCallback._reader.abandonAllParts();
      Assert.fail();
    }
    catch (ReaderFinishedException readerFinishedException)
    {
      //pass
    }
    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 1);
    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks
        .get(0)._streamError instanceof NullPointerException);
    try
    {
      _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._singlePartMIMEReader.requestPartData();
      Assert.fail();
    }
    catch (PartFinishedException partFinishedException)
    {
      //pass
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  private CountDownLatch executeRequestPartialReadWithException(final ByteString requestPayload, final int chunkSize,
      final String contentTypeHeader) throws Exception
  {
    final EntityStream entityStream = mock(EntityStream.class);
    final ReadHandle readHandle = mock(ReadHandle.class);

    //We have to use the AtomicReference holder technique to modify the current remaining buffer since the inner class
    //in doAnswer() can only access final variables.
    final AtomicReference<MultiPartMIMEReader.R2MultiPartMIMEReader> r2Reader =
        new AtomicReference<MultiPartMIMEReader.R2MultiPartMIMEReader>();

    //This takes the place of VariableByteStringWriter if we were to use R2 directly.
    final VariableByteStringViewer variableByteStringViewer = new VariableByteStringViewer(requestPayload, chunkSize);

    doAnswer(new Answer()
    {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable
      {
        final MultiPartMIMEReader.R2MultiPartMIMEReader reader = r2Reader.get();
        Object[] args = invocation.getArguments();

        //will always be 1 since MultiPartMIMEReader only does _rh.request(1)
        final int chunksRequested = (Integer) args[0];

        for (int i = 0; i < chunksRequested; i++)
        {

          //Our tests will run into a stack overflow unless we use a thread pool here to fire off the callbacks.
          //Especially in cases where the chunk size is 1. When the chunk size is one, the MultiPartMIMEReader
          //ends up doing many _rh.request(1) since each write is only 1 byte.
          //R2 uses a different technique to avoid stack overflows here which is unnecessary to emulate.
          _scheduledExecutorService.submit(new Runnable()
          {
            @Override
            public void run()
            {
              ByteString clientData = variableByteStringViewer.onWritePossible();
              if (clientData.equals(ByteString.empty()))
              {
                reader.onDone();
              } else
              {
                reader.onDataAvailable(clientData);
              }
            }
          });
        }

        return null;
      }
    }).when(readHandle).request(isA(Integer.class));

    doAnswer(new Answer()
    {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable
      {
        Object[] args = invocation.getArguments();
        final MultiPartMIMEReader.R2MultiPartMIMEReader reader = (MultiPartMIMEReader.R2MultiPartMIMEReader) args[0];
        r2Reader.set(reader);
        //R2 calls init immediately upon setting the reader
        reader.onInit(readHandle);
        return null;
      }
    }).when(entityStream).setReader(isA(MultiPartMIMEReader.R2MultiPartMIMEReader.class));

    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);

    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn(contentTypeHeader);

    final CountDownLatch latch = new CountDownLatch(1);

    _reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);
    _currentMultiPartMIMEReaderCallback = new MultiPartMIMEExceptionReaderCallbackImpl(latch, _reader);
    _reader.registerReaderCallback(_currentMultiPartMIMEReaderCallback);

    return latch;
  }

  private static class SinglePartMIMEExceptionReaderCallbackImpl implements SinglePartMIMEReaderCallback
  {
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    Throwable _streamError = null;
    final CountDownLatch _countDownLatch;

    static boolean throwOnPartDataAvailable = false;
    static boolean throwOnFinished = false;
    static boolean throwOnAbandoned = false;

    static void resetAllFlags()
    {
      throwOnPartDataAvailable = false;
      throwOnFinished = false;
      throwOnAbandoned = false;
    }

    SinglePartMIMEExceptionReaderCallbackImpl(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader,
        final CountDownLatch countDownLatch)
    {
      _singlePartMIMEReader = singlePartMIMEReader;
      _countDownLatch = countDownLatch;
    }

    @Override
    public void onPartDataAvailable(ByteString partData)
    {
      if (throwOnPartDataAvailable)
      {
        throw new NullPointerException();
      } else if (throwOnAbandoned)
      {
        _singlePartMIMEReader.abandonPart();
        return;
      } else
      {
        _singlePartMIMEReader.requestPartData();
      }
    }

    @Override
    public void onFinished()
    {
      if (throwOnFinished)
      {
        throw new NullPointerException();
      }
    }

    @Override
    public void onAbandoned()
    {
      //We only reached here due to the presence of throwOnAbandoned == true
      throw new NullPointerException();
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      _streamError = throwable;
    }
  }

  private static class MultiPartMIMEExceptionReaderCallbackImpl implements MultiPartMIMEReaderCallback
  {
    final List<SinglePartMIMEExceptionReaderCallbackImpl> _singlePartMIMEReaderCallbacks =
        new ArrayList<SinglePartMIMEExceptionReaderCallbackImpl>();
    Throwable _streamError = null;
    final CountDownLatch _latch;
    final MultiPartMIMEReader _reader;

    static boolean throwOnNewPart = false;
    static boolean throwOnFinished = false;
    static boolean throwOnAbandoned = false;

    static void resetAllFlags()
    {
      throwOnNewPart = false;
      throwOnFinished = false;
      throwOnAbandoned = false;
    }

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
    {
      if (throwOnNewPart)
      {
        throw new NullPointerException();
      }

      if (throwOnAbandoned)
      {
        _reader.abandonAllParts();
        return;
      }

      SinglePartMIMEExceptionReaderCallbackImpl singlePartMIMEReaderCallback =
          new SinglePartMIMEExceptionReaderCallbackImpl(singlePartMIMEReader, _latch);
      singlePartMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
      _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);

      singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished()
    {
      if (throwOnFinished)
      {
        throw new NullPointerException();
      }
    }

    @Override
    public void onAbandoned()
    {
      //We only reached here due to the presence of throwOnAbandoned == true
      throw new NullPointerException();
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      _streamError = throwable;
      _latch.countDown();
    }

    MultiPartMIMEExceptionReaderCallbackImpl(final CountDownLatch latch, final MultiPartMIMEReader reader)
    {
      _latch = latch;
      _reader = reader;
    }
  }
}