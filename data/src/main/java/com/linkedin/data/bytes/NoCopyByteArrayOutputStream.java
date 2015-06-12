package com.linkedin.data.bytes;

/**
 * Created by kvidhani on 8/14/15.
 */

import java.io.ByteArrayOutputStream;

/**
 * This class is intended for internal use only. The output stream should not be passed around after
 * the construction; otherwise the internal representation of the ByteString would change, voiding the
 * immutability guarantee.
 */
class NoCopyByteArrayOutputStream extends ByteArrayOutputStream
{
  byte[] getBytes()
  {
    return super.buf;
  }

  int getBytesCount()
  {
    return super.count;
  }
}