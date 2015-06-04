package com.linkedin.multipart.writer;

import com.linkedin.data.ByteString;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;


/**
 * Created by kvidhani on 6/3/15.
 */
class MultiPartMIMEUtils {

  static final byte[] CRLF = "\r\n".getBytes();
  private static final char[] MULTIPART_CHARS =
      "-_1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  static ByteString serializedHeaders(final Map<String, String> headers) {

    final StringBuffer headerBuffer = new StringBuffer();
    for (final Map.Entry<String, String> header : headers.entrySet()) {
      headerBuffer.append(formattedHeader(header.getKey(), header.getValue()));
    }

    //Headers should always be 7 bit ASCII according to the RFC
    return ByteString.copyString(headerBuffer.toString(), Charset.forName("US-ASCII"));
  }

  static String formattedHeader(final String name, final String value) {
    return ((name == null ? "" : name) + ": " + (null == value ? "" : value) + CRLF);
  }

  static String generateBoundary() {
    final StringBuilder buffer = new StringBuilder();
    final Random rand = new Random();
    //The RFC limit is 70 characters, so we will create a boundary that is randomly
    //between 50 to 60 characters. This should ensure that we never see the boundary within the request
    final int count = rand.nextInt(11) + 50; // a random size from 50 to 60
    for (int i = 0; i < count; i++) {
      buffer.append(MULTIPART_CHARS[rand.nextInt(MULTIPART_CHARS.length)]);
    }
    //RFC 2046 states a limited character set for the boundary
    //Which means we can encode using ASCII todo
    return buffer.toString();
  }
}
