package com.linkedin.multipart;

import com.linkedin.data.ByteString;
import com.linkedin.multipart.reader.exceptions.IllegalMimeFormatException;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;


/**
 * Created by kvidhani on 6/3/15.
 */
public class MultiPartMIMEUtils {

  //R2 uses a case insensitive TreeMap so the casing here for the Content-Type header does not matter
  public static final String CONTENT_TYPE_HEADER = "Content-Type";
  public static final String MULTIPART_PREFIX = "multipart/";
  public static final String BOUNDARY_PARAMETER = "boundary";
  public static final Byte SPACE_BYTE = 32;
  public static final Byte TAB_BYTE = 9;

  public static final String CRLF_STRING = "\r\n";
  public static final byte[] CRLF_BYTES = "\r\n".getBytes();
  public static final List<Byte> CRLF_BYTE_LIST = new ArrayList<Byte>();

  public static final String CONSECUTIVE_CRLFS_STRING = "\r\n\r\n";
  public static final byte[] CONSECUTIVE_CRLFS_BYTES = "\r\n\r\n".getBytes();
  public static final List<Byte> CONSECUTIVE_CRLFS_BYTE_LIST = new ArrayList<Byte>();
  static {
    for (final byte b : CONSECUTIVE_CRLFS_BYTES) {
      CONSECUTIVE_CRLFS_BYTE_LIST.add(b);
    }

    for (final byte b :CRLF_BYTES) {
      CRLF_BYTE_LIST.add(b);
    }
  }

  private static final char[] MULTIPART_CHARS =
      "-_1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  public static ByteString serializedHeaders(final Map<String, String> headers) {

    final StringBuffer headerBuffer = new StringBuffer();
    for (final Map.Entry<String, String> header : headers.entrySet()) {
      headerBuffer.append(formattedHeader(header.getKey(), header.getValue()));
    }

    //Headers should always be 7 bit ASCII according to the RFC. If characters provided in the header
    //are do not constitute a valid ASCII character, then this Charset will place (U+FFFD) or the
    //replacement character which is used to replace an unknown or unrepresentable character.
    return ByteString.copyString(headerBuffer.toString(), Charset.forName("US-ASCII"));
  }

  public static String formattedHeader(final String name, final String value) {
    return ((name == null ? "" : name) + ": " + (null == value ? "" : value) + CRLF_STRING);
  }

  public static String generateBoundary()
  {
    final StringBuilder buffer = new StringBuilder();
    final Random rand = new Random();
    //The RFC limit is 70 characters, so we will create a boundary that is randomly
    //between 50 to 60 characters. This should ensure that we never see the boundary within the request
    final int count = rand.nextInt(11) + 50; // a random size from 50 to 60
    for (int i = 0; i < count; i++) {
      buffer.append(MULTIPART_CHARS[rand.nextInt(MULTIPART_CHARS.length)]);
    }
    //RFC 2046 states a limited character set for the boundary but we don't have to explicitly encode to ASCII
    //since Unicode is backward compatible with ASCII
    return buffer.toString();
  }

  public static String buildMIMEContentTypeHeader(final String mimeType, final String boundary,
      final Map<String, String> contentTypeParameters) {

    final StringBuilder contentTypeBuilder = new StringBuilder();
    contentTypeBuilder.append(MULTIPART_PREFIX).append(mimeType);
    //As per the RFC, parameters of the Content-Type header are separated by semi colons
    contentTypeBuilder.append("; ").append(BOUNDARY_PARAMETER).append("=").append(boundary);

    for (final Map.Entry<String, String> parameter : contentTypeParameters.entrySet()) {
      //Note we ignore the provided boundary parameter
      if(!parameter.getKey().trim().equalsIgnoreCase(BOUNDARY_PARAMETER)) {
        contentTypeBuilder.append("; ").append(parameter.getKey().trim()).append("=")
            .append(parameter.getValue().trim());
      }
    }

    return contentTypeBuilder.toString();
  }

  //todo we can only do so much validation, we need javadocs to mention we make some assumptions
  //todo - how can clients deal with these exceptions?
  public static String extractBoundary(final String contentTypeHeader) throws IllegalMimeFormatException
  {
    if(!contentTypeHeader.contains(";"))
    {
      throw new IllegalMimeFormatException("Improperly formatted Content-Type header. "
          + "Expected at least one parameter in addition to the content type.");
    }

    final String[] contentTypeParameters = contentTypeHeader.split(";");

    //In case someone used something like bOuNdArY
    //todo - do we want to be able to provide this as a getter inside of MultiPartMIMEReader?
    final Map<String, String> parameterMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    for (final String parameter : contentTypeParameters) {

      //We don't need the first bit here.
      if (parameter.startsWith(MULTIPART_PREFIX)) continue;

      final String trimmedParameter = parameter.trim();
      //According to the RFC, there could be an '=' character in the boundary so we can't just split on =.
      //We find the first equals and then go from there. It should also be noted that the RFC does allow
      //boundaries to start and end with quotes.
      final int firstEquals = trimmedParameter.indexOf("=");
      //We throw an exception if there is no equals sign, or if the equals is the first character or if the
      //equals is the last character.
      if (firstEquals == 0 || firstEquals == -1 || firstEquals == trimmedParameter.length() - 1) {
        throw new IllegalMimeFormatException("Invalid parameter format.");
      }

      //Todo - Should we actually go through each character in the boundary and make sure it matches acceptable
      //characters from the RFC? This may not be necessary.
      final String parameterKey = trimmedParameter.substring(0, firstEquals);
      String parameterValue = trimmedParameter.substring(firstEquals + 1, trimmedParameter.length());
      if(parameterValue.charAt(0) == '"') {
        if(parameterValue.charAt(parameterValue.length()-1) != '"') {
          throw new IllegalMimeFormatException("Invalid parameter format.");
        }
        //Remove the leading and trailing '"'
        parameterValue = parameterValue.substring(1, parameterValue.length()-1);
      }

      if (parameterMap.containsKey(parameterKey)) {
        throw new IllegalMimeFormatException("Invalid parameter format. Multiple decelerations of the same parameter!");
      }
      parameterMap.put(parameterKey, parameterValue);
    }

    final String boundaryValue = parameterMap.get(BOUNDARY_PARAMETER);

    if (boundaryValue == null) {
      throw new IllegalMimeFormatException("No boundary parameter found!");
    }

    return boundaryValue;
  }
}
