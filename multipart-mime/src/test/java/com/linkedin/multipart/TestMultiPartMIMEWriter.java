package com.linkedin.multipart;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.StreamRequest;
import com.linkedin.r2.message.streaming.FullEntityReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.activation.DataSource;
import javax.mail.BodyPart;
import javax.mail.Header;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Created by kvidhani on 7/7/15.
 */
//Using Javax.mail on the server side to verify the integrity of our RFC implementation of the MultiPartMIMEWriter

//Read in all the bytes that the writer generates into memory and then use javax mail to verify things look correct
//There is no way to use mockito to mock EntitStreams.newEntityStream()
//Therefore we have no choice but to use R2's functionality here.
public class TestMultiPartMIMEWriter {

  private static ScheduledExecutorService scheduledExecutorService;
  private static final int TEST_TIMEOUT = 90000;

  byte[] normalBodyData;
  Map<String, String> normalBodyHeaders;

  byte[] headerLessBodyData;

  Map<String, String> bodyLessHeaders;

  MultiPartMIMEDataPartImpl normalBody;
  MultiPartMIMEDataPartImpl headerLessBody;
  MultiPartMIMEDataPartImpl bodyLessBody;

  @BeforeTest
  public void setup() {

    normalBodyData = "abc".getBytes();
    normalBodyHeaders = new HashMap<String, String>();
    normalBodyHeaders.put("simpleheader", "simplevalue");

    //Second body has no headers
    headerLessBodyData = "def".getBytes();

    //Third body has only headers
    bodyLessHeaders = new HashMap<String, String>();
    normalBodyHeaders.put("header1", "value1");
    normalBodyHeaders.put("header2", "value2");
    normalBodyHeaders.put("header3", "value3");

    normalBody =
        new MultiPartMIMEDataPartImpl(ByteString.copy(normalBodyData), normalBodyHeaders);

    headerLessBody =
        new MultiPartMIMEDataPartImpl(ByteString.copy(headerLessBodyData), Collections.<String, String>emptyMap());

    bodyLessBody =
        new MultiPartMIMEDataPartImpl(ByteString.empty(), bodyLessHeaders);

    scheduledExecutorService = Executors.newScheduledThreadPool(10);
  }

  @AfterTest
  public void shutDown() {
    scheduledExecutorService.shutdownNow();
  }



  @DataProvider(name = "singleDataSources")
  public Object[][] singleDataSources() throws Exception
  {

    return new Object[][] {
        {ByteString.copy(normalBodyData), normalBodyHeaders},
        {ByteString.copy(headerLessBodyData), Collections.<String, String>emptyMap()},
        {ByteString.empty(), bodyLessHeaders}
    };
  }

  @Test(dataProvider = "singleDataSources")
  public void testSingleDataSource(final ByteString body, final Map<String, String> headers) throws Exception {

    final MultiPartMIMEDataPartImpl expectedMultiPartMIMEDataPart =
        new MultiPartMIMEDataPartImpl(body, headers);

    final MultiPartMIMEInputStream simpleDataSource =
        new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(body.copyBytes()), scheduledExecutorService, headers).build();

    final MultiPartMIMEWriter multiPartMIMEWriter =
        new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder("preamble", "epilogue").appendDataSource(simpleDataSource).build();

    //final AtomicReference
    final FutureCallback<ByteString> futureCallback = new FutureCallback<ByteString>();
    final FullEntityReader fullEntityReader = new FullEntityReader(futureCallback);
    multiPartMIMEWriter.getEntityStream().setReader(fullEntityReader);
    futureCallback.get(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    //todo this may change
    final StreamRequest multiPartMIMEStreamRequest = new MultiPartMIMEStreamRequestBuilder(URI.create("localhost"),
        "mixed", multiPartMIMEWriter, Collections.<String, String>emptyMap()).build();

    JavaxMailMultiPartMIMEReader javaxMailMultiPartMIMEReader =
        new JavaxMailMultiPartMIMEReader(multiPartMIMEStreamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER),
            futureCallback.get());
    javaxMailMultiPartMIMEReader.parseRequestIntoParts();


    List<MultiPartMIMEDataPartImpl> dataSourceList = javaxMailMultiPartMIMEReader._dataSourceList;

    Assert.assertEquals(dataSourceList.size(), 1);
    Assert.assertEquals(dataSourceList.get(0), expectedMultiPartMIMEDataPart);

  }


  @Test
  public void testMultipleDataSources() throws Exception {

    final List<MultiPartMIMEDataPartImpl> expectedParts = new ArrayList<MultiPartMIMEDataPartImpl>();
    expectedParts.add(normalBody);
    expectedParts.add(normalBody);
    expectedParts.add(headerLessBody);
    expectedParts.add(normalBody);
    expectedParts.add(bodyLessBody);
    expectedParts.add(headerLessBody);
    expectedParts.add(headerLessBody);
    expectedParts.add(headerLessBody);
    expectedParts.add(normalBody);
    expectedParts.add(bodyLessBody);

    final List<MultiPartMIMEDataSource> inputStreamDataSources = new ArrayList<MultiPartMIMEDataSource>();
    inputStreamDataSources.add(new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(normalBodyData), scheduledExecutorService, normalBodyHeaders).build());
    inputStreamDataSources.add(new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(normalBodyData), scheduledExecutorService, normalBodyHeaders).build());
    inputStreamDataSources.add(new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(headerLessBodyData), scheduledExecutorService, Collections.<String, String>emptyMap()).build());
    inputStreamDataSources.add(new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(normalBodyData), scheduledExecutorService, normalBodyHeaders).build());
    inputStreamDataSources.add(new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(new byte[0]), scheduledExecutorService, bodyLessHeaders).build());
    inputStreamDataSources.add(new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(headerLessBodyData), scheduledExecutorService, Collections.<String, String>emptyMap()).build());
    inputStreamDataSources.add(new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(headerLessBodyData), scheduledExecutorService, Collections.<String, String>emptyMap()).build());
    inputStreamDataSources.add(new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(headerLessBodyData), scheduledExecutorService, Collections.<String, String>emptyMap()).build());
    inputStreamDataSources.add(new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(normalBodyData), scheduledExecutorService, normalBodyHeaders).build());
    inputStreamDataSources.add(new MultiPartMIMEInputStream.Builder(new ByteArrayInputStream(new byte[0]), scheduledExecutorService, bodyLessHeaders).build());

    final MultiPartMIMEWriter multiPartMIMEWriter =
        new MultiPartMIMEWriter.MultiPartMIMEWriterBuilder("preamble", "epilogue").appendDataSources(inputStreamDataSources).build();

    final FutureCallback<ByteString> futureCallback = new FutureCallback<ByteString>();
    final FullEntityReader fullEntityReader = new FullEntityReader(futureCallback);
    multiPartMIMEWriter.getEntityStream().setReader(fullEntityReader);
    futureCallback.get(TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    //todo this may change
    final StreamRequest multiPartMIMEStreamRequest = new MultiPartMIMEStreamRequestBuilder(URI.create("localhost"),
        "mixed", multiPartMIMEWriter, Collections.<String, String>emptyMap()).build();

    JavaxMailMultiPartMIMEReader javaxMailMultiPartMIMEReader =
        new JavaxMailMultiPartMIMEReader(multiPartMIMEStreamRequest.getHeader(MultiPartMIMEUtils.CONTENT_TYPE_HEADER),
            futureCallback.get());
    javaxMailMultiPartMIMEReader.parseRequestIntoParts();


    List<MultiPartMIMEDataPartImpl> dataSourceList = javaxMailMultiPartMIMEReader._dataSourceList;

    Assert.assertEquals(dataSourceList.size(), 10);
    for (int i = 0;i<dataSourceList.size(); i++) {
      Assert.assertEquals(dataSourceList.get(i), expectedParts.get(i));
    }

  }



  private static class JavaxMailMultiPartMIMEReader
  {
    final String _contentTypeHeaderValue;
    final ByteString _payload;

    final List<MultiPartMIMEDataPartImpl> _dataSourceList = new ArrayList<MultiPartMIMEDataPartImpl>();

    private JavaxMailMultiPartMIMEReader(final String contentTypeHeaderValue, final ByteString paylaod)
    {
      _contentTypeHeaderValue = contentTypeHeaderValue;
      _payload = paylaod;
    }

    @SuppressWarnings("rawtypes")
    private void parseRequestIntoParts()
    {
      final DataSource dataSource = new DataSource()
      {
        @Override
        public InputStream getInputStream() throws IOException
        {
          return new ByteArrayInputStream(_payload.copyBytes());
        }

        @Override
        public OutputStream getOutputStream() throws IOException
        {
          return null;
        }

        @Override
        public String getContentType()
        {
          return _contentTypeHeaderValue;
        }

        @Override
        public String getName()
        {
          return null;
        }
      };

      try
      {
        final MimeMultipart mimeBody = new MimeMultipart(dataSource);
        for (int i = 0; i < mimeBody.getCount(); i++)
        {
          final BodyPart bodyPart = mimeBody.getBodyPart(i);
          try
          {
            //For our purposes, javax mail converts the body part's content (based on headers) into a string
            final ByteString partData = ByteString.copyString((String) bodyPart.getContent(), Charset.defaultCharset());

            final Map<String, String> partHeaders = new HashMap<String, String>();
            final Enumeration allHeaders = bodyPart.getAllHeaders();
            while (allHeaders.hasMoreElements())
            {
              final Header header = (Header) allHeaders.nextElement();
              partHeaders.put(header.getName(), header.getValue());
            }
            final MultiPartMIMEDataPartImpl tempDataSource = new MultiPartMIMEDataPartImpl(partData, partHeaders);
            _dataSourceList.add(tempDataSource);
          }
          catch (Exception exception)
          {
            Assert.fail("Failed to read body content due to " + exception);
          }
        }
      }
      catch (MessagingException messagingException)
      {
        Assert.fail("Failed to read in request multipart mime body");
      }
    }
  }

  public static class MultiPartMIMEDataPartImpl
  {
    final ByteString _partData;
    final Map<String, String> _headers;

    public MultiPartMIMEDataPartImpl(final ByteString partData, final Map<String, String> headers)
    {
      if (partData == null)
      {
        _partData = ByteString.empty();
      }
      else
      {
        _partData = partData;
      }
      _headers = headers;
    }

    public ByteString getPartData()
    {
      return _partData;
    }

    public Map<String, String> getPartHeaders()
    {
      return _headers;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o)
      {
        return true;
      }

      if (!(o instanceof MultiPartMIMEDataPartImpl))
      {
        return false;
      }

      final MultiPartMIMEDataPartImpl that = (MultiPartMIMEDataPartImpl) o;

      if(!_headers.equals(that.getPartHeaders()))
      {
        return false;
      }

      if(!_partData.equals(that.getPartData()))
      {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = _partData != null ? _partData.hashCode() : 0;
      result = 31 * result + (_headers != null ? _headers.hashCode() : 0);
      return result;
    }
  }

}
