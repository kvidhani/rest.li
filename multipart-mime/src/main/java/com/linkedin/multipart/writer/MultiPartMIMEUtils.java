package com.linkedin.multipart.writer;

import com.linkedin.data.ByteString;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;


/**
 * Created by kvidhani on 6/3/15.
 */
class MultiPartMIMEUtils {

  static final String CONTENT_TYPE_HEADER = "Content-Type";
  static final String MULTIPART_PREFIX = "multipart/";
  static final String BOUNDARY_PARAMETER = "boundary";
  static final byte[] CRLF = "\r\n".getBytes();
  private static final char[] MULTIPART_CHARS =
      "-_1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  static ByteString serializedHeaders(final Map<String, String> headers) {

    final StringBuffer headerBuffer = new StringBuffer();
    for (final Map.Entry<String, String> header : headers.entrySet()) {
      headerBuffer.append(formattedHeader(header.getKey(), header.getValue()));
    }

    //Headers should always be 7 bit ASCII according to the RFC. If characters provided in the header
    //are do not constitute a valid ASCII character, then this Charset will place (U+FFFD) or the
    //replacement character which is used to replace an unknown or unrepresentable character.
    return ByteString.copyString(headerBuffer.toString(), Charset.forName("US-ASCII"));
  }

  static String formattedHeader(final String name, final String value) {
    return ((name == null ? "" : name) + ": " + (null == value ? "" : value) + CRLF);
  }

  static String generateBoundary()
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

  static String buildContentTypeHeader(final String mimeType, final String boundary,
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
}
