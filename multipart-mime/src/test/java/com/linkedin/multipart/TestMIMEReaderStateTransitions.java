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


import com.linkedin.multipart.exceptions.PartBindException;
import com.linkedin.multipart.exceptions.PartFinishedException;
import com.linkedin.multipart.exceptions.PartNotInitializedException;
import com.linkedin.multipart.exceptions.ReaderNotInitializedException;
import com.linkedin.multipart.exceptions.StreamBusyException;
import com.linkedin.multipart.exceptions.ReaderFinishedException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.EntityStream;

import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * These tests will verify that the correct exceptions are thrown in the face of unorthodox clients.
 *
 * @author Karim Vidhani
 */
public class TestMIMEReaderStateTransitions
{
  //MultiPartMIMEReader exceptions:
  @Test
  public void testRegisterCallbackMultiPartMIMEReader()
  {
    final EntityStream entityStream = mock(EntityStream.class);
    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER))
        .thenReturn("multipart/mixed; boundary=\"--123\"");
    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);

    //Test each possible exception:

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.FINISHED);
    try
    {
      reader.registerReaderCallback(null);
      Assert.fail();
    }
    catch (ReaderFinishedException readerFinishedException)
    {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.READING_EPILOGUE);
    try
    {
      reader.registerReaderCallback(null);
      Assert.fail();
    }
    catch (ReaderFinishedException readerFinishedException)
    {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.CALLBACK_BOUND_AND_READING_PREAMBLE);
    try
    {
      reader.registerReaderCallback(null);
      Assert.fail();
    }
    catch (StreamBusyException streamBusyException)
    {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.ABANDONING);
    try
    {
      reader.registerReaderCallback(null);
      Assert.fail();
    }
    catch (StreamBusyException streamBusyException)
    {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.READING_PARTS); //This is a desired top level reader state
    final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader =
        reader.new SinglePartMIMEReader(Collections.<String, String>emptyMap());
    singlePartMIMEReader
        .setState(MultiPartMIMEReader.SingleReaderState.REQUESTED_DATA); //This is a undesired single part state
    reader.setCurrentSinglePartMIMEReader(singlePartMIMEReader);
    try
    {
      reader.registerReaderCallback(null);
      Assert.fail();
    }
    catch (StreamBusyException streamBusyException)
    {
    }
  }

  @Test
  public void testAbandonAllPartsMultiPartMIMEReader()
  {
    final EntityStream entityStream = mock(EntityStream.class);
    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER))
        .thenReturn("multipart/mixed; boundary=\"--123\"");

    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);

    //Test each possible exception:

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.CREATED);
    try
    {
      reader.abandonAllParts();
      Assert.fail();
    }
    catch (ReaderNotInitializedException readerNotInitializedException)
    {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.FINISHED);
    try
    {
      reader.abandonAllParts();
      Assert.fail();
    }
    catch (ReaderFinishedException readerFinishedException)
    {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.READING_EPILOGUE);
    try
    {
      reader.abandonAllParts();
      Assert.fail();
    }
    catch (ReaderFinishedException readerFinishedException)
    {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.CALLBACK_BOUND_AND_READING_PREAMBLE);
    try
    {
      reader.abandonAllParts();
      Assert.fail();
    }
    catch (StreamBusyException streamBusyException)
    {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.ABANDONING);
    try
    {
      reader.abandonAllParts();
      Assert.fail();
    }
    catch (StreamBusyException streamBusyException)
    {
    }

    reader
        .setState(MultiPartMIMEReader.MultiPartReaderState.READING_PARTS); //This is the desired top level reader state
    final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader =
        reader.new SinglePartMIMEReader(Collections.<String, String>emptyMap());
    singlePartMIMEReader
        .setState(MultiPartMIMEReader.SingleReaderState.REQUESTED_DATA); //This is a undesired single part state
    reader.setCurrentSinglePartMIMEReader(singlePartMIMEReader);
    try
    {
      reader.abandonAllParts();
      Assert.fail();
    }
    catch (StreamBusyException streamBusyException)
    {
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  //SinglePartMIMEReader exceptions:

  @Test
  public void testRegisterSinglePartMIMEReaderCallbackTwice()
  {
    final EntityStream entityStream = mock(EntityStream.class);

    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER))
        .thenReturn("multipart/mixed; boundary=\"--123\"");

    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);

    final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader =
        reader.new SinglePartMIMEReader(Collections.<String, String>emptyMap());
    singlePartMIMEReader
        .setState(MultiPartMIMEReader.SingleReaderState.REQUESTED_DATA); //This is a undesired single part state
    try
    {
      singlePartMIMEReader.registerReaderCallback(null);
      Assert.fail();
    }
    catch (PartBindException partBindException)
    {
    }
  }

  @Test
  public void testSinglePartMIMEReaderVerifyState()
  {
    //This will cover abandonPart() and most of requestPartData().
    //The caveat is that requestPartData() requires a callback to be registered. This
    //will be covered in the next test.

    final EntityStream entityStream = mock(EntityStream.class);

    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER))
        .thenReturn("multipart/mixed; boundary=\"--123\"");

    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);

    final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader =
        reader.new SinglePartMIMEReader(Collections.<String, String>emptyMap());

    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.FINISHED);
    try
    {
      singlePartMIMEReader.verifyState();
      Assert.fail();
    }
    catch (PartFinishedException partFinishedException)
    {
    }

    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.REQUESTED_DATA);
    try
    {
      singlePartMIMEReader.verifyState();
      Assert.fail();
    }
    catch (StreamBusyException streamBusyException)
    {
    }

    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.REQUESTED_ABORT);
    try
    {
      singlePartMIMEReader.verifyState();
      Assert.fail();
    }
    catch (StreamBusyException streamBusyException)
    {
    }
  }

  @Test
  public void testSinglePartMIMEReaderRequestData()
  {
    final EntityStream entityStream = mock(EntityStream.class);
    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER))
        .thenReturn("multipart/mixed; boundary=\"--123\"");
    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);

    final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader =
        reader.new SinglePartMIMEReader(Collections.<String, String>emptyMap());

    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.CREATED);
    try
    {
      singlePartMIMEReader.requestPartData();
      Assert.fail();
    }
    catch (PartNotInitializedException partNotInitializedException)
    {
    }
  }
}