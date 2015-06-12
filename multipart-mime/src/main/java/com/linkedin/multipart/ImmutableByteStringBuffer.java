package com.linkedin.multipart;

import com.linkedin.data.ByteString;

import java.util.List;

/**
 * Created by kvidhani on 8/12/15.
 */
class ImmutableByteStringBuffer extends ByteStringBuffer
{
  ImmutableByteStringBuffer(final int byteOffsetBeginning,
                            final int byteOffsetEnd,
                            final List<ByteString> byteStringList)
  {
    super(byteOffsetBeginning, byteOffsetEnd, byteStringList);
  }

  @Override
  byte byteAtIndex(final int index) {
    return super.byteAtIndex(index);
  }

  @Override
  int size() {
    return super.size();
  }

  @Override
  int indexOfBytes(byte[] targetBytes) {
    return super.indexOfBytes(targetBytes);
  }

  @Override
  ByteStringBuffer subBuffer(int startIndex, int endIndex, BufferType bufferType) {
    return super.subBuffer(startIndex, endIndex, bufferType);
  }

  @Override
  void trimFromBeginning(int newStartIndex) {
    throw new UnsupportedOperationException("Addition is not allowed on sub ByteStringBuffers");
  }

  @Override
  void add(ByteString byteString) {
    throw new UnsupportedOperationException("Addition is not allowed on sub ByteStringBuffers");
  }

}
