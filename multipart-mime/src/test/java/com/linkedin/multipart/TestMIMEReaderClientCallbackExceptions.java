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
import com.linkedin.multipart.exceptions.MultiPartReaderFinishedException;
import com.linkedin.multipart.exceptions.SinglePartFinishedException;
import com.linkedin.r2.filter.R2Constants;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.multipart.utils.MIMETestUtils.*;


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

    return new Object[][]
        {
            {1, bodyPartList},
            {R2Constants.DEFAULT_DATA_CHUNK_SIZE, bodyPartList}
        };
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
    catch (MultiPartReaderFinishedException multiPartReaderFinishedException)
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
    catch (MultiPartReaderFinishedException multiPartReaderFinishedException)
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
      catch (SinglePartFinishedException singlePartFinishedException)
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
    catch (MultiPartReaderFinishedException multiPartReaderFinishedException)
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
    CountDownLatch countDownLatch = executeRequestPartialReadWithException(requestPayload, chunkSize, multiPartMimeBody.getContentType());

    countDownLatch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._streamError instanceof NullPointerException);
    //Verify this are unusable.
    try
    {
      _currentMultiPartMIMEReaderCallback._reader.abandonAllParts();
      Assert.fail();
    }
    catch (MultiPartReaderFinishedException multiPartReaderFinishedException)
    {
      //pass
    }

    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 1);
    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._streamError instanceof NullPointerException);
    try
    {
      _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._singlePartMIMEReader.requestPartData();
      Assert.fail();
    }
    catch (SinglePartFinishedException singlePartFinishedException)
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
    CountDownLatch countDownLatch = executeRequestPartialReadWithException(requestPayload, chunkSize, multiPartMimeBody.getContentType());

    countDownLatch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._streamError instanceof NullPointerException);
    //Verify this are unusable.
    try
    {
      _currentMultiPartMIMEReaderCallback._reader.abandonAllParts();
      Assert.fail();
    }
    catch (MultiPartReaderFinishedException multiPartReaderFinishedException)
    {
      //pass
    }

    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 1);
    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._streamError instanceof NullPointerException);
    try
    {
      _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._singlePartMIMEReader.requestPartData();
      Assert.fail();
    }
    catch (SinglePartFinishedException singlePartFinishedException)
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
    CountDownLatch countDownLatch = executeRequestPartialReadWithException(requestPayload, chunkSize, multiPartMimeBody.getContentType());

    countDownLatch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._streamError instanceof NullPointerException);
    //Verify these are unusable.
    try
    {
      _currentMultiPartMIMEReaderCallback._reader.abandonAllParts();
      Assert.fail();
    }
    catch (MultiPartReaderFinishedException multiPartReaderFinishedException)
    {
      //pass
    }
    Assert.assertEquals(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.size(), 1);
    Assert.assertTrue(_currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._streamError instanceof NullPointerException);
    try
    {
      _currentMultiPartMIMEReaderCallback._singlePartMIMEReaderCallbacks.get(0)._singlePartMIMEReader.requestPartData();
      Assert.fail();
    }
    catch (SinglePartFinishedException singlePartFinishedException)
    {
      //pass
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  private CountDownLatch executeRequestPartialReadWithException(final ByteString requestPayload, final int chunkSize,
                                                                final String contentTypeHeader) throws Exception
  {
    mockR2AndWrite(requestPayload, chunkSize, contentTypeHeader);
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
      }
      else if (throwOnAbandoned)
      {
        _singlePartMIMEReader.abandonPart();
        return;
      }
      else
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
    final List<SinglePartMIMEExceptionReaderCallbackImpl> _singlePartMIMEReaderCallbacks = new ArrayList<SinglePartMIMEExceptionReaderCallbackImpl>();
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