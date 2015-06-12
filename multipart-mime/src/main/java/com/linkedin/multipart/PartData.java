package com.linkedin.multipart;

import com.linkedin.data.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by kvidhani on 8/9/15.
 */
public class PartData extends ByteStringBuffer
{
  PartData(final int byteOffsetBeginning,
           final int byteOffsetEnd,
           final List<ByteString> byteStringList) {
    super(byteOffsetBeginning, byteOffsetEnd, byteStringList);
  }

  @Override
  public byte byteAtIndex(final int index) {
    return super.byteAtIndex(index);
  }

  @Override
  public int size() {
    return super.size();
  }

  /**
   *
   * @return
   * @throws IOException
   */
  public byte[] copyBytes() throws IOException
  {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    for (int i = 0; i < _byteStringList.size(); i++) {
      final ByteString currentByteString = _byteStringList.get(i);
      //Take into account the offset in the first and final ByteString
      int byteStringStart = i == 0 ? _byteOffsetBeginning : 0;
      int byteStringLength = i == _byteStringList.size() - 1 ? _byteOffsetEnd : currentByteString.length();
      byteArrayOutputStream.write(currentByteString.slice(byteStringStart, byteStringLength - byteStringStart).copyBytes());
    }
    return byteArrayOutputStream.toByteArray();
  }
}
