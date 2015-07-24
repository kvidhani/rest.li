package com.linkedin.multipart;

import com.linkedin.multipart.reader.exceptions.PartBindException;
import com.linkedin.multipart.reader.exceptions.PartFinishedException;
import com.linkedin.multipart.reader.exceptions.PartNotInitializedException;
import com.linkedin.multipart.reader.exceptions.ReaderNotInitializedException;
import com.linkedin.multipart.reader.exceptions.StreamBusyException;
import com.linkedin.multipart.reader.exceptions.StreamFinishedException;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.EntityStream;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Created by kvidhani on 7/22/15.
 */
public class TestMultiPartMIMEReaderStateTransitions {

  //These tests will verify that the correct exceptions are thrown in the face of unorthodox clients.

  ///////////////////////////////////////////////////////////////////////////////////////

  //MultiPartMIMEReader exceptions:


  @Test
  public void testRegisterCallbackMultiPartMIMEReader() {

    final EntityStream entityStream = mock(EntityStream.class);

    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn("multipart/mixed; boundary=\"--123\"");

    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);

    //Test each possible exception:

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.FINISHED);
    try {
      reader.registerReaderCallback(null);
      Assert.fail();
    } catch (StreamFinishedException streamFinishedException) {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.READING_EPILOGUE);
    try {
      reader.registerReaderCallback(null);
      Assert.fail();
    } catch (StreamFinishedException streamFinishedException) {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.CALLBACK_BOUND_AND_READING_PREAMBLE);
    try {
      reader.registerReaderCallback(null);
      Assert.fail();
    } catch (StreamBusyException streamBusyException) {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.ABANDONING);
    try {
      reader.registerReaderCallback(null);
      Assert.fail();
    } catch (StreamBusyException streamBusyException) {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.READING_PARTS); //This is a desired top level reader state
    final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader = reader.new SinglePartMIMEReader(
        Collections.<String, String>emptyMap());
    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.REQUESTED_DATA); //This is a undesired single part state
    reader.setCurrentSinglePartMIMEReader(singlePartMIMEReader);
    try {
      reader.registerReaderCallback(null);
      Assert.fail();
    } catch (StreamBusyException streamBusyException) {
    }
  }


  @Test
  public void testAbandonAllPartsMultiPartMIMEReader() {

    final EntityStream entityStream = mock(EntityStream.class);
    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn("multipart/mixed; boundary=\"--123\"");

    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);

    //Test each possible exception:

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.CREATED);
    try {
      reader.abandonAllParts();
      Assert.fail();
    } catch (ReaderNotInitializedException readerNotInitializedException) {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.FINISHED);
    try {
      reader.abandonAllParts();
      Assert.fail();
    } catch (StreamFinishedException streamFinishedException) {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.READING_EPILOGUE);
    try {
      reader.abandonAllParts();
      Assert.fail();
    } catch (StreamFinishedException streamFinishedException) {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.CALLBACK_BOUND_AND_READING_PREAMBLE);
    try {
      reader.abandonAllParts();
      Assert.fail();
    } catch (StreamBusyException streamBusyException) {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.ABANDONING);
    try {
      reader.abandonAllParts();
      Assert.fail();
    } catch (StreamBusyException streamBusyException) {
    }

    reader.setState(MultiPartMIMEReader.MultiPartReaderState.READING_PARTS); //This is the desired top level reader state
    final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader = reader.new SinglePartMIMEReader(
        Collections.<String, String>emptyMap());
    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.REQUESTED_DATA); //This is a undesired single part state
    reader.setCurrentSinglePartMIMEReader(singlePartMIMEReader);
    try {
      reader.abandonAllParts();
      Assert.fail();
    } catch (StreamBusyException streamBusyException) {
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////




  @Test
  public void testRegisterSinglePartMIMEReaderCallbackTwice() {

    final EntityStream entityStream = mock(EntityStream.class);

    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn("multipart/mixed; boundary=\"--123\"");

    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);

    final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader = reader.new SinglePartMIMEReader(
        Collections.<String, String>emptyMap());
    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.REQUESTED_DATA); //This is a undesired single part state
    try {
      singlePartMIMEReader.registerReaderCallback(null);
      Assert.fail();
    } catch (PartBindException partBindException) {
    }
  }


  @Test
  public void testSinglePartMIMEReaderVerifyState() {

    //This will cover both requestPartData() and abandonPart();

    final EntityStream entityStream = mock(EntityStream.class);

    final StreamRequest streamRequest = mock(StreamRequest.class);
    when(streamRequest.getEntityStream()).thenReturn(entityStream);
    when(streamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER)).thenReturn("multipart/mixed; boundary=\"--123\"");

    MultiPartMIMEReader reader = MultiPartMIMEReader.createAndAcquireStream(streamRequest);

    final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader = reader.new SinglePartMIMEReader(
        Collections.<String, String>emptyMap());

    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.FINISHED);
    try {
      singlePartMIMEReader.verifyState();
      Assert.fail();
    } catch (PartFinishedException partFinishedException) {
    }

    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.CREATED);
    try {
      singlePartMIMEReader.verifyState();
      Assert.fail();
    } catch (PartNotInitializedException partNotInitializedException) {
    }

    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.REQUESTED_DATA);
    try {
      singlePartMIMEReader.verifyState();
      Assert.fail();
    } catch (StreamBusyException streamBusyException) {
    }

    singlePartMIMEReader.setState(MultiPartMIMEReader.SingleReaderState.REQUESTED_ABORT);
    try {
      singlePartMIMEReader.verifyState();
      Assert.fail();
    } catch (StreamBusyException streamBusyException) {
    }
  }

}
