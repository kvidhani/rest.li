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

package com.linkedin.multipart.utils;


import com.google.common.collect.ImmutableMap;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.MultiPartMIMEDataSource;
import com.linkedin.multipart.MultiPartMIMEInputStream;
import com.linkedin.multipart.MultiPartMIMEReader;
import com.linkedin.multipart.MultiPartMIMEReaderCallback;
import com.linkedin.multipart.SinglePartMIMEReaderCallback;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.mail.internet.ContentType;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.ParameterList;

import org.testng.Assert;


/**
 * Shared data sources and utilities for tests.
 *
 * @author Karim Vidhani
 */
public final class MIMETestUtils
{
  public static final String HEADER_CONTENT_TYPE = "Content-Type";
  public static final String TEXT_PLAIN_CONTENT_TYPE = "text/plain";
  public static final String BINARY_CONTENT_TYPE = "application/octet-stream";

  //For the abandoning tests:
  public static final String ABANDON_HEADER = "AbandonMe";

  //Header values for different server side behavior:

  //Top level abandon all after registering a callback with the MultiPartMIMEReader. This abandon call will happen
  //upon the first invocation on onNewPart():
  public static final String TOP_ALL_WITH_CALLBACK = "TOP_ALL_WITH_CALLBACK";

  //Top level abandon without registering a callback with the MultipartMIMEReader:
  public static final String TOP_ALL_NO_CALLBACK = "TOP_ALL_NO_CALLBACK";

  //Single part abandons all individually but doesn't use a callback:
  public static final String SINGLE_ALL_NO_CALLBACK = "SINGLE_ALL_NO_CALLBACK";

  //Single part abandons the first 6 (using registered callbacks) and then the top level abandons all of remaining:
  public static final String SINGLE_PARTIAL_TOP_REMAINING = "SINGLE_PARTIAL_TOP_REMAINING";

  //Single part alternates between consumption and abandoning the first 6 parts (using registered callbacks), then top
  //level abandons all of remaining. This means that parts 0, 2, 4 will be consumed and parts 1, 3, 5 will be abandoned.
  public static final String SINGLE_ALTERNATE_TOP_REMAINING = "SINGLE_ALTERNATE_TOP_REMAINING";

  //Single part abandons all individually (using registered callbacks):
  public static final String SINGLE_ALL = "SINGLE_ALL";

  //Single part alternates between consumption and abandoning all the way through (using registered callbacks):
  public static final String SINGLE_ALTERNATE = "SINGLE_ALTERNATE";

  //Javax mail data sources
  public static MimeBodyPart tinyDataSource;
  //Represents a tiny part with no headers. Used exclusively for the stack overflow test.
  public static MimeBodyPart smallDataSource; //Represents a small part with headers and a body composed of simple text
  public static MimeBodyPart largeDataSource; //Represents a large part with headers and a body composed of simple text
  public static MimeBodyPart headerLessBody; //Represents a part with a body and no headers
  public static MimeBodyPart bodyLessBody; //Represents a part with headers but no body
  public static MimeBodyPart bytesBody; //Represents a part with bytes
  public static MimeBodyPart purelyEmptyBody; //Represents a part with no headers and no body

  //Non javax, custom data sources
  public static MIMEDataPart bodyA;
  public static MIMEDataPart bodyB;
  public static MIMEDataPart bodyC;
  public static MIMEDataPart bodyD;
  public static MIMEDataPart body1;
  public static MIMEDataPart body2;
  public static MIMEDataPart body3;
  public static MIMEDataPart body4;
  public static MIMEDataPart body5;

  //Disable instantiation
  private MIMETestUtils()
  {
  }

  //Javax mail always includes a final, trailing CRLF after the final boundary. Meaning something like
  //--myFinalBoundary--/r/n
  //
  //This trailing CRLF is not considered part of the final boundary and is, presumably, some sort of default
  //epilogue. We want to remove this, otherwise all of our data sources in all of our tests will always have some sort
  //of epilogue at the end and we won't have any tests where the data sources end with JUST the final boundary.
  public static ByteString trimTrailingCRLF(final ByteString javaxMailPayload)
  {
    //Assert the trailing CRLF does
    final byte[] javaxMailPayloadBytes = javaxMailPayload.copyBytes();
    //Verify, in case the version of javax mail is changed, that the last two bytes are still CRLF (13 and 10).
    Assert.assertEquals(javaxMailPayloadBytes[javaxMailPayloadBytes.length - 2], 13);
    Assert.assertEquals(javaxMailPayloadBytes[javaxMailPayloadBytes.length - 1], 10);
    return javaxMailPayload.copySlice(0, javaxMailPayload.length() - 2);
  }

  public static List<Integer> generatePrimeNumbers(final int limit)
  {
    final List<Integer> primeNumberList = new ArrayList<Integer>();
    for (int i = 1; i < limit; i++)
    {
      boolean isPrimeNumber = true;

      //Check to see if the number is prime
      for (int j = 2; j < i; j++)
      {
        if (i % j == 0)
        {
          isPrimeNumber = false;
          break;
        }
      }

      if (isPrimeNumber)
      {
        primeNumberList.add(i);
      }
    }

    return primeNumberList;
  }

  static
  {
    //Non javax mail sources:
    final byte[] bodyAbytes = "bodyA".getBytes();
    final Map<String, String> bodyAHeaders = ImmutableMap.of("headerA", "valueA");
    bodyA = new MIMEDataPart(ByteString.copy(bodyAbytes), bodyAHeaders);

    final byte[] bodyBbytes = "bodyB".getBytes();
    final Map<String, String> bodyBHeaders = ImmutableMap.of("headerB", "valueB");
    bodyB = new MIMEDataPart(ByteString.copy(bodyBbytes), bodyBHeaders);

    //body c has no headers
    final byte[] bodyCbytes = "bodyC".getBytes();
    bodyC = new MIMEDataPart(ByteString.copy(bodyCbytes), Collections.<String, String>emptyMap());

    final byte[] bodyDbytes = "bodyD".getBytes();
    final Map<String, String> bodyDHeaders = ImmutableMap.of("headerD", "valueD");
    bodyD = new MIMEDataPart(ByteString.copy(bodyDbytes), bodyDHeaders);

    final byte[] body1bytes = "body1".getBytes();
    final Map<String, String> body1Headers = ImmutableMap.of("header1", "value1");
    body1 = new MIMEDataPart(ByteString.copy(body1bytes), body1Headers);

    final byte[] body2bytes = "body2".getBytes();
    final Map<String, String> body2Headers = ImmutableMap.of("header2", "value2");
    body2 = new MIMEDataPart(ByteString.copy(body2bytes), body2Headers);

    //body 3 is completely empty
    body3 = new MIMEDataPart(ByteString.empty(), Collections.<String, String>emptyMap());

    final byte[] body4bytes = "body4".getBytes();
    final Map<String, String> body4Headers = ImmutableMap.of("header4", "value4");
    body4 = new MIMEDataPart(ByteString.copy(body4bytes), body4Headers);

    final byte[] localInputStreamBytes = "local input stream".getBytes();
    final Map<String, String> localInputStreamHeaders = ImmutableMap.of("local1", "local2");
    body5 = new MIMEDataPart(ByteString.copy(localInputStreamBytes), localInputStreamHeaders);

    //Now create the javax data sources:
    try
    {
      //Tiny body.
      {
        final String body = "1";
        final MimeBodyPart dataPart = new MimeBodyPart();
        final ContentType contentType = new ContentType(TEXT_PLAIN_CONTENT_TYPE);
        dataPart.setContent(body, contentType.getBaseType());
        tinyDataSource = dataPart;
      }

      //Small body.
      {
        final String body = "A small body";
        final MimeBodyPart dataPart = new MimeBodyPart();
        final ContentType contentType = new ContentType(TEXT_PLAIN_CONTENT_TYPE);
        dataPart.setContent(body, contentType.getBaseType());
        dataPart.setHeader(HEADER_CONTENT_TYPE, contentType.toString());
        dataPart.setHeader("SomeCustomHeader", "SomeCustomValue");
        smallDataSource = dataPart;
      }

      //Large body. Something bigger then the size of the boundary with folded headers.
      {
        final String body =
            "Has at possim tritani laoreet, vis te meis verear. Vel no vero quando oblique, eu blandit placerat nec, vide facilisi recusabo nec te. Veri labitur sensibus eum id. Quo omnis "
                + "putant erroribus ad, nonumes copiosae percipit in qui, id cibo meis clita pri. An brute mundi quaerendum duo, eu aliquip facilisis sea, eruditi invidunt dissentiunt eos ea.";
        final MimeBodyPart dataPart = new MimeBodyPart();
        final ContentType contentType = new ContentType(TEXT_PLAIN_CONTENT_TYPE);
        dataPart.setContent(body, contentType.getBaseType());
        //Modify the content type header to use folding. We will also use multiple headers that use folding to verify
        //the integrity of the reader. Note that the Content-Type header uses parameters which are key/value pairs
        //separated by '='. Note that we do not use two consecutive CRLFs anywhere since our implementation
        //does not support this.
        final StringBuffer contentTypeBuffer = new StringBuffer(contentType.toString());
        contentTypeBuffer.append(";\r\n\t\t\t");
        contentTypeBuffer.append("parameter1= value1");
        contentTypeBuffer.append(";\r\n   \t");
        contentTypeBuffer.append("parameter2= value2");

        //This is a custom header which is folded. It does not use parameters so it's values are separated by commas.
        final StringBuffer customHeaderBuffer = new StringBuffer();
        customHeaderBuffer.append("CustomValue1");
        customHeaderBuffer.append(",\r\n\t  \t");
        customHeaderBuffer.append("CustomValue2");
        customHeaderBuffer.append(",\r\n ");
        customHeaderBuffer.append("CustomValue3");

        dataPart.setHeader(HEADER_CONTENT_TYPE, contentTypeBuffer.toString());
        dataPart.setHeader("AnotherCustomHeader", "AnotherCustomValue");
        dataPart.setHeader("FoldedHeader", customHeaderBuffer.toString());
        largeDataSource = dataPart;
      }

      //Header-less body. This has a body but no headers.
      {
        final String body = "A body without any headers.";
        final MimeBodyPart dataPart = new MimeBodyPart();
        final ContentType contentType = new ContentType(TEXT_PLAIN_CONTENT_TYPE);
        dataPart.setContent(body, contentType.getBaseType());
        headerLessBody = dataPart;
      }

      //Body-less body. This has no body but does have headers, some of which are folded.
      {
        final MimeBodyPart dataPart = new MimeBodyPart();
        final ParameterList parameterList = new ParameterList();
        parameterList.set("AVeryVeryVeryVeryLongHeader", "AVeryVeryVeryVeryLongValue");
        parameterList.set("AVeryVeryVeryVeryLongHeader2", "AVeryVeryVeryVeryLongValue2");
        parameterList.set("AVeryVeryVeryVeryLongHeader3", "AVeryVeryVeryVeryLongValue3");
        parameterList.set("AVeryVeryVeryVeryLongHeader4", "AVeryVeryVeryVeryLongValue4");
        final ContentType contentType = new ContentType("text", "plain", parameterList);
        dataPart.setContent("", contentType.getBaseType());
        dataPart.setHeader(HEADER_CONTENT_TYPE, contentType.toString());
        dataPart.setHeader("YetAnotherCustomHeader", "YetAnotherCustomValue");
        bodyLessBody = dataPart;
      }

      //Bytes body. A body that uses a content type different then just text/plain.
      {
        final byte[] body = new byte[20];
        for (int i = 0; i < body.length; i++)
        {
          body[i] = (byte) i;
        }
        final MimeBodyPart dataPart = new MimeBodyPart();
        final ContentType contentType = new ContentType(BINARY_CONTENT_TYPE);
        dataPart.setContent(body, contentType.getBaseType());
        dataPart.setHeader(HEADER_CONTENT_TYPE, contentType.toString());
        bytesBody = dataPart;
      }

      //Purely empty body. This has no body or headers.
      {
        final MimeBodyPart dataPart = new MimeBodyPart();
        final ContentType contentType = new ContentType(TEXT_PLAIN_CONTENT_TYPE);
        dataPart.setContent("", contentType.getBaseType()); //Mail requires content so we do a bit of a hack here.
        purelyEmptyBody = dataPart;
      }
    }
    catch (Exception exception)
    {
      Assert.fail();
    }
  }

  //The chaining tests will use these:
  public static List<MultiPartMIMEDataSource> generateInputStreamDataSources(final int chunkSize,
                                                                             final ExecutorService executorService)
  {
    final MultiPartMIMEInputStream bodyADataSource =
        new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(bodyA.getPartData().copyBytes()),
            executorService, bodyA.getPartHeaders()).withWriteChunkSize(chunkSize).build();

    final MultiPartMIMEInputStream bodyBDataSource =
        new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(bodyB.getPartData().copyBytes()),
            executorService, bodyB.getPartHeaders()).withWriteChunkSize(chunkSize).build();

    final MultiPartMIMEInputStream bodyCDataSource =
        new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(bodyC.getPartData().copyBytes()),
            executorService, bodyC.getPartHeaders()).withWriteChunkSize(chunkSize).build();

    final MultiPartMIMEInputStream bodyDDataSource =
        new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(bodyD.getPartData().copyBytes()),
            executorService, bodyD.getPartHeaders()).withWriteChunkSize(chunkSize).build();

    final List<MultiPartMIMEDataSource> dataSources = new ArrayList<MultiPartMIMEDataSource>();
    dataSources.add(bodyADataSource);
    dataSources.add(bodyBDataSource);
    dataSources.add(bodyCDataSource);
    dataSources.add(bodyDDataSource);

    return dataSources;
  }

  //These are general purpose callbacks that simply read bytes and store them in memory:
  public static class SinglePartMIMEFullReaderCallback implements SinglePartMIMEReaderCallback
  {
    final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    final ByteArrayOutputStream _byteArrayOutputStream = new ByteArrayOutputStream();
    Map<String, String> _headers;
    ByteString _finishedData = null;

    public SinglePartMIMEFullReaderCallback(final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
    {
      _singlePartMIMEReader = singlePartMIMEReader;
      _headers = singlePartMIMEReader.dataSourceHeaders();
    }

    public MultiPartMIMEReader.SinglePartMIMEReader getSinglePartMIMEReader()
    {
      return _singlePartMIMEReader;
    }

    public Map<String, String> getHeaders()
    {
      return _headers;
    }

    public ByteString getFinishedData()
    {
      return _finishedData;
    }

    @Override
    public void onPartDataAvailable(ByteString partData)
    {
      try
      {
        _byteArrayOutputStream.write(partData.copyBytes());
      }
      catch (IOException ioException)
      {
        onStreamError(ioException);
      }
      _singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished()
    {
      _finishedData = ByteString.copy(_byteArrayOutputStream.toByteArray());
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

  public static class MultiPartMIMEFullReaderCallback implements MultiPartMIMEReaderCallback
  {
    final List<SinglePartMIMEFullReaderCallback> _singlePartMIMEReaderCallbacks =
        new ArrayList<SinglePartMIMEFullReaderCallback>();

    public MultiPartMIMEFullReaderCallback()
    {
    }

    public List<SinglePartMIMEFullReaderCallback> getSinglePartMIMEReaderCallbacks()
    {
      return _singlePartMIMEReaderCallbacks;
    }

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
    {
      SinglePartMIMEFullReaderCallback singlePartMIMEReaderCallback = new SinglePartMIMEFullReaderCallback(singlePartMIMEReader);
      singlePartMIMEReader.registerReaderCallback(singlePartMIMEReaderCallback);
      _singlePartMIMEReaderCallbacks.add(singlePartMIMEReaderCallback);
      singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished()
    {
      //We don't have to do anything here.
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
}