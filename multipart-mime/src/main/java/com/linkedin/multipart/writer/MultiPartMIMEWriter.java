package com.linkedin.multipart.writer;

import com.linkedin.multipart.exceptions.IllegalContentTypeException;
import com.linkedin.r2.message.streaming.EntityStream;
import com.linkedin.r2.message.streaming.EntityStreams;
import com.linkedin.r2.message.streaming.WriteHandle;
import com.linkedin.r2.message.streaming.Writer;
import com.sun.mail.util.LineOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;


/**
 * Created by kvidhani on 5/18/15.
 */
//todo mention thread safety for all of this
//todo redo javadocs

public final class MultiPartMIMEWriter {

  public static final String HEADER_CONTENT_TYPE = "Content-Type";
  public static final String MULTI_PART_MIME_PREFIX = "multipart/";
  public static final String DEFAULT_CONTENT_TYPE = MULTI_PART_MIME_PREFIX + "mixed";
  public static final String BOUNDARY_PARAMETER = "boundary";
  private static final char[] MULTIPART_CHARS =
      "-_1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  private final R2MultiPartMIMEWriter _writer;
  private final EntityStream _entityStream;
  private final List<MultiPartMIMEDataSource> _dataSources;
  private final Map<String, String> _topLevelHeaders;
  private final String _boundary;
  private WriteHandle _writeHandle;

  private class R2MultiPartMIMEWriter implements Writer {

    private int _currentDataSource;

    @Override
    public void onInit(WriteHandle wh) {
      _writeHandle = wh;
      _currentDataSource = 0;
    }

    @Override
    public void onWritePossible() {
      //todo check to see if write handle is null?

      //On the first try we will have to write out the top level headers
      //If content-type header is missing, we use the default

      //Iterate through data sources one by one, get the bytes, and write to the write handle
      //make sure to fulfill all of the bytes requested
      while (_writeHandle.remaining() > 0) {
        final MultiPartMIMEDataSource currentDataSource = _dataSources.get(_currentDataSource);
        final DataSourceHandle dataSourceHandle = new DataSourceHandleImpl(_writeHandle);
        LineOutputStream a = null;
      }

      //todo handle preamble
    }

    @Override
    public void onAbort(Throwable e) {
      //Abort all data sources from the current data source going forward.
      //Also the current DataSourceHandle should be set to ABORTED
    }

    R2MultiPartMIMEWriter() {
    }
  }

  //Javadocs will mention the need for clients to pass in content-type header
  public MultiPartMIMEWriter(final MultiPartMIMEDataSource dataSource, final Map<String, String> headers)
      throws IllegalContentTypeException {

    this(Arrays.asList(dataSource), headers);
  }

  public MultiPartMIMEWriter(final List<MultiPartMIMEDataSource> dataSources, final Map<String, String> headers)
      throws IllegalContentTypeException {

    _dataSources = dataSources;
    _writer = new R2MultiPartMIMEWriter();
    _entityStream = EntityStreams.newEntityStream(_writer);
    _topLevelHeaders = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    _topLevelHeaders.putAll(headers);

    //We need to extract the content-type header
    //If the Content-Type header is present, we assume it is properly constructed. If its not, we throw
    if (_topLevelHeaders.containsKey(HEADER_CONTENT_TYPE)) {

      final String contentTypeValue = _topLevelHeaders.get(HEADER_CONTENT_TYPE);

      //Note that RFC 2045 states that parameters within Content-Type are semicolon separated
      //This is different from RFC 2616 that states that multiple values for the same header can be comma separated

      //todo - Javadoc to mention that semicolons can't be a part of any of the parameters for content-type
      final String[] headerParameters = contentTypeValue.split(";");
      //Check the presence of both multipart and boundary
      final Map<String, String> contentTypeParameterMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
      for (final String parameter : headerParameters)
      {
        final String trimmedParameter = parameter.trim();
        final String[] parameterKeyValue = trimmedParameter.split("=");
        if(parameterKeyValue.length!=2)
        {
          //Some sort of malformed boundary so we throw
          throw new IllegalContentTypeException("Malformed Content-Type header");
        }

        contentTypeParameterMap.put(parameterKeyValue[0].trim(), parameterKeyValue[1].trim());
      }

      boolean foundMIMEtype = false;
      boolean foundBoundary = false;
      for (final Map.Entry<String, String> keyValuePair : contentTypeParameterMap.entrySet())
      {
        final String lowercaseKey = keyValuePair.getKey().toLowerCase();
        if(lowercaseKey.startsWith(MULTI_PART_MIME_PREFIX.toLowerCase())) {
          foundMIMEtype = true;
        }
        if(lowercaseKey.startsWith(BOUNDARY_PARAMETER.toLowerCase())) {
          foundBoundary = true;
        }
      }

      if(!foundMIMEtype)
      {
        throw new IllegalContentTypeException("Multipart mime type not specified");
      }

      if(!foundBoundary)
      {
        throw new IllegalContentTypeException("Boundary not provided");
      }

      //At this point its a valid Content-Type header, so we extract the boundary
    }
    else
    {
      //We need to use the default Content-Type and generate our own boundary
      final String generatedBoundary = generateBoundary();
      _boundary = generatedBoundary;
      final StringBuffer headerValue = new StringBuffer();
      headerValue.append(DEFAULT_CONTENT_TYPE + "; " + "boundary=" + generatedBoundary);
      _topLevelHeaders.put(HEADER_CONTENT_TYPE, headerValue.toString());
    }
  }

  public EntityStream getEntityStream() {
    return _entityStream;
  }

  private String generateBoundary() {
    final StringBuilder buffer = new StringBuilder();
    final Random rand = new Random();
    final int count = rand.nextInt(11) + 30; // a random size from 30 to 40
    for (int i = 0; i < count; i++) {
      buffer.append(MULTIPART_CHARS[rand.nextInt(MULTIPART_CHARS.length)]);
    }
    return buffer.toString();
  }
}
