package com.linkedin.data.bytes;


import com.linkedin.data.ByteString;
import com.linkedin.data.Data;
import com.linkedin.util.ArgumentUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Created by kvidhani on 8/13/15.
 */
//todo mention in super class the copies that occur

  //todo figure out if you want this guy to work on instances whose end boundary is
  //less then the size of last byte string
  //todo improve byte at index
public final class CompoundByteString extends ByteString
{
  private final List<ByteString> _byteStringList;
  private final int _byteOffsetBeginning; //Offset from first byte string
  private final int _byteOffsetEnd; //offset in last byte string
  private final int _length;

  private CompoundByteString(final int byteOffsetBeginning, final int byteOffsetEnd,
      final List<ByteString> byteStringList) {

    _byteStringList = new ArrayList<ByteString>(byteStringList);
    _byteOffsetBeginning = byteOffsetBeginning;
    _byteOffsetEnd = byteOffsetEnd;

    if (_byteStringList.size() == 0) {
      _length = 0;
    } else
    {
      int totalCount = 0;
      for (int i = 0; i < _byteStringList.size(); i++)
      {
        totalCount += _byteStringList.get(i).length();
      }

      totalCount -= _byteOffsetBeginning;
      totalCount -= _byteStringList.get(_byteStringList.size() - 1).length() - _byteOffsetEnd;
      _length = totalCount;
    }
  }

  public static ByteString create(final int byteOffsetBeginning,
                           final List<ByteString> byteStringList)
  {
    ArgumentUtil.notNull(byteStringList, "Null ByteString list not allowed");
    if (byteOffsetBeginning < 0) {
      throw new IllegalArgumentException("Invalid beginning offset specified");
    }

    return new CompoundByteString(byteOffsetBeginning, byteStringList.get(byteStringList.size()-1).length(), byteStringList);
  }

  //Add and return a new CompoundByteString
  public CompoundByteString addAndCreateNew(final ByteString byteString) {
    if (byteString == null) {
      throw new IllegalArgumentException("Null ByteString provided");
    }

    return new CompoundByteString(_byteOffsetBeginning, byteString.length(), _byteStringList);
  }

  public CompoundByteString trimAndCreateNew(final int newStartIndex) {

    if (newStartIndex < 0) {
      throw new IllegalArgumentException("Invalid new start index specified");
    }

    //We should always have this invariant true for this class
    //todo do we want this?
    assert(_byteOffsetEnd == _byteStringList.get(_byteStringList.size() -1).length());
    int currentCount = 0;
    final List<ByteString> newList = new ArrayList<ByteString>(_byteStringList);
    for (int i = 0; i < _byteStringList.size(); i++) {
      //Take into account the offset in the first and final ByteString
      int byteStringStart = i == 0 ? _byteOffsetBeginning : 0;
      for (int j = byteStringStart; j < _byteStringList.get(i).length(); j++) {
        currentCount++;
        if (currentCount == newStartIndex) {
          //We have found the index in our ByteString buffer where we want to trim up until.
          for (int k = 0; k < i; k++) {
            newList.remove(k);
          }
          return new CompoundByteString(j, _byteOffsetEnd, newList);
        }
      }
    }

    //We use this technique of calculating the limit instead of using size() to save an extra
    //iteration through all of our data.
    throw new IllegalArgumentException("Provided new start index larger then size of ByteString buffer");
  }

















  /**
   * Returns the number of bytes in this {@link com.linkedin.data.ByteString}.
   *
   * @return the number of bytes in this {@link com.linkedin.data.ByteString}
   */
  @Override
  public int length()
  {
   return _length;
  }

  @Override
  public int indexOfBytes(final byte[] targetBytes)
  {
    if (targetBytes == null)
    {
      throw new IllegalArgumentException("Target byte array is null");
    }

    if (targetBytes.length == 0)
    {
      return 0;
    }

    outer:
    for (int i = 0; i< length() - targetBytes.length + 1; i++) {
      for (int k = 0; k < targetBytes.length; k++) {
        if (byteAtIndex(i+k) != targetBytes[k]) {
          continue outer;
        }
      }
      return i;
    }
    return -1;
  }

  @Override
  public byte byteAtIndex(final int index)
  {
    if(index<0)
    {
      throw new IllegalArgumentException("Provided index cannot be negative");
    }

    int currentCount = 0;
    //This will run fast since the only thing we will need do is iterate through the ByteString counts which should
    //realistically be very few.
    for (int i = 0; i < _byteStringList.size(); i++) {
      final ByteString currentByteString = _byteStringList.get(i);
      //Take into account the offset in the first and final ByteString
      int byteStringStart = i == 0 ? _byteOffsetBeginning : 0;
      int byteStringLength = i == _byteStringList.size() - 1 ? _byteOffsetEnd : currentByteString.length();
      if(currentCount + byteStringLength > index) {
        //We are at the right ByteString, so now we get the right byte
        return currentByteString.byteAtIndex(index - currentCount);
      }
      currentCount += byteStringLength - byteStringStart;
    }

    throw new IllegalArgumentException("Provided index is out of upper range");
  }


  /**
   * Checks whether this {@link com.linkedin.data.ByteString} is empty or not.
   * @return true for an empty {@link com.linkedin.data.ByteString}, false otherwise
   */
  @Override
  public boolean isEmpty()
  {
    return _length == 0;
  }

  /**
   * Returns a copy of the bytes in this {@link com.linkedin.data.ByteString}. Changes to the returned byte[] will not be
   * reflected in this {@link com.linkedin.data.ByteString}.<p>
   *
   * Where possible prefer other methods for accessing the underlying bytes, such as
   * {@link #asByteBuffer()}, {@link #write(java.io.OutputStream)}, or {@link #asString(Charset)}.
   * The first two make no copy of the byte array, while the last minimizes the amount of copying
   * (constructing a String from a byte[] always involves copying).
   *
   * @return a copy of the bytes in this {@link com.linkedin.data.ByteString}
   */
  @Override
  public byte[] copyBytes()
  {
    try
    {
      final NoCopyByteArrayOutputStream byteArrayOutputStream = new NoCopyByteArrayOutputStream();
      for (int i = 0; i < _byteStringList.size(); i++)
      {
        final ByteString currentByteString = _byteStringList.get(i);
        //Take into account the offset in the first and final ByteString
        int byteStringStart = i == 0 ? _byteOffsetBeginning : 0;
        int byteStringLength = i == _byteStringList.size() - 1 ? _byteOffsetEnd : currentByteString.length();
        byteArrayOutputStream
            .write(currentByteString.slice(byteStringStart, byteStringLength - byteStringStart).copyBytes());
      }
      return byteArrayOutputStream.getBytes();
    } catch (IOException ioException) {
      throw new IllegalStateException("Serious error in constructing a copy of the bytes.");
    }
  }

  /**
   * Copy the bytes in this {@link com.linkedin.data.ByteString} to the provided byte[] starting at the specified offset.
   *
   * Where possible prefer other methods for accessing the underlying bytes, such as
   * {@link #asByteBuffer()}, {@link #write(java.io.OutputStream)}, or {@link #asString(Charset)}.
   * The first two make no copy of the byte array, while the last minimizes the amount of copying
   * (constructing a String from a byte[] always involves copying).
   *  @param dest is the destination to copy the bytes in this {@link com.linkedin.data.ByteString} to.
   * @param offset is the starting offset in the destination to receive the copy.
   */
  @Override
  public void copyBytes(byte[] dest, int offset)
  {
    System.arraycopy(copyBytes(), 0, dest, offset, _length);
  }

  /**
   * Returns a read only {@link java.nio.ByteBuffer} view of this {@link com.linkedin.data.ByteString}. This method makes no copy
   *
   * @return read only {@link java.nio.ByteBuffer} view of this {@link com.linkedin.data.ByteString}.
   */
  @Override
  public ByteBuffer asByteBuffer()
  {
    return ByteBuffer.wrap(copyBytes(), 0, _length).asReadOnlyBuffer();
  }

  /**
   * Return a String representation of the bytes in this {@link com.linkedin.data.ByteString}, decoded using the supplied
   * charset.
   *
   * @param charsetName the name of the charset to use to decode the bytes
   * @return the String representation of this {@link com.linkedin.data.ByteString}
   */
  @Override
  public String asString(String charsetName)
  {
    return asString(Charset.forName(charsetName));
  }

  /**
   * Return a String representation of the bytes in this {@link com.linkedin.data.ByteString}, decoded using the supplied
   * charset.
   *
   * @param charset the charset to use to decode the bytes
   * @return the String representation of this {@link com.linkedin.data.ByteString}
   */
  @Override
  public String asString(Charset charset)
  {
    return new String(copyBytes(), 0, _length, charset);
  }

  /**
   * Return an Avro representation of the bytes in this {@link com.linkedin.data.ByteString}.
   *
   * @return the String representation of this {@link com.linkedin.data.ByteString}
   */
  @Override
  public String asAvroString()
  {
    return Data.bytesToString(copyBytes(), 0, _length);
  }

  /**
   * Return an {@link java.io.InputStream} view of the bytes in this {@link com.linkedin.data.ByteString}.
   *
   * @return an {@link java.io.InputStream} view of the bytes in this {@link com.linkedin.data.ByteString}
   */
  @Override
  public InputStream asInputStream()
  {
    return new ByteArrayInputStream(copyBytes(), 0, _length);
  }

  /**
   * Writes this {@link com.linkedin.data.ByteString} to a stream without copying the underlying byte[].
   *
   * @param out the stream to write the bytes to
   *
   * @throws java.io.IOException if an error occurs while writing to the stream
   */
  @Override
  public void write(OutputStream out) throws IOException
  {
    for (int i = 0; i < _byteStringList.size(); i++) {
      final ByteString currentByteString = _byteStringList.get(i);
      //Take into account the offset in the first and final ByteString
      int byteStringStart = i == 0 ? _byteOffsetBeginning : 0;
      int byteStringLength = i == _byteStringList.size() - 1 ? _byteOffsetEnd : currentByteString.length();
      for (int j = byteStringStart; j < byteStringLength; j++) {
        out.write(currentByteString.byteAtIndex(j));
      }
    }
  }

  /**
   * Returns a slice of ByteString.
   * This create a "view" of this ByteString, which holds the entire content of the original ByteString. If your code
   * only needs a small portion of a large ByteString and is not interested in the rest of that ByteString, it is better
   * to use {@link #copySlice} method.
   *
   * @param offset the starting point of the slice
   * @param length the length of the slice
   * @return a slice of ByteString backed by the same backing byte array
   * @throws IndexOutOfBoundsException if offset or length is negative, or offset + length is larger than the length
   * of this ByteString
   */
  @Override
  public ByteString slice(int offset, int length)
  {
    ArgumentUtil.checkBounds(_length, offset, length);
    final int endIndex = length + offset;

    int newByteOffsetBeginning = 0;
    int newByteOffsetEnd = 0;
    final List<ByteString> newByteStringList = new ArrayList<ByteString>();

      newByteStringList.addAll(_byteStringList);
      int currentCount = 0;
    outerloop:
      for (int i = 0; i < _byteStringList.size(); i++)
      {
        final ByteString currentByteString = _byteStringList.get(i);
        //Take into account the offset in the first and final ByteString
        int byteStringStart = i == 0 ? _byteOffsetBeginning : 0;
        int byteStringLength = i == _byteStringList.size() - 1 ? _byteOffsetEnd : currentByteString.length();
        for (int j = byteStringStart; j < byteStringLength; j++)
        {
          currentCount++;
          if (currentCount == offset)
          {
            for (int k = 0; k < i; k++)
            {
              newByteStringList.remove(k);
            }
            newByteOffsetBeginning = j;
          }
          if (currentCount == endIndex)
          {
            for (int k = i; k < _byteStringList.size(); k++)
            {
              newByteStringList.remove(k);
            }
            newByteOffsetEnd = j;
            break outerloop; //This jumps to the end so we can return
          }
        }
      }
      return new CompoundByteString(newByteOffsetBeginning, newByteOffsetEnd, _byteStringList);
  }

  /**
   * Returns a slice of ByteString backed by a new byte array.
   * This copies the content from the desired portion of the original ByteString and does not hold reference to the
   * original ByteString.
   *
   * @param offset the starting point of the slice
   * @param length the length of the slice
   * @return a slice of ByteString backed by a new byte array
   * @throws IndexOutOfBoundsException if offset or length is negative, or offset + length is larger than the length
   * of this ByteString
   */
  @Override
  public ByteString copySlice(int offset, int length)
  {
    ArgumentUtil.checkBounds(_length, offset, length);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    for (int i = offset; i < length + offset; i++) {
      byteArrayOutputStream.write(byteAtIndex(i));
    }
    return new ByteStringImpl(byteArrayOutputStream.toByteArray());
  }

  @Override
  public int hashCode()
  {
    int result = 1;
    for (int i = 0; i < _byteStringList.size(); i++)
    {
      result = result * 31 + _byteStringList.get(i).hashCode();
    }
    return result;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o)
    {
      return true;
    }
    if (!(o instanceof CompoundByteString))
    {
      return false;
    }

    CompoundByteString that = (CompoundByteString) o;

    if (_byteOffsetBeginning != that._byteOffsetBeginning)
    {
      return false;
    }
    if (_byteOffsetEnd != that._byteOffsetEnd)
    {
      return false;
    }
    if (_length != that._length)
    {
      return false;
    }
    if (!_byteStringList.equals(that._byteStringList))
    {
      return false;
    }

    return true;
  }

  @Override
  public String toString()
  {
    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < _byteStringList.size(); i++) {
      final ByteString currentByteString = _byteStringList.get(i);
      //Take into account the offset in the first and final ByteString
      int byteStringStart = i == 0 ? _byteOffsetBeginning : 0;
      int byteStringLength = i == _byteStringList.size() - 1 ? _byteOffsetEnd : currentByteString.length();
      //This won't copy
      stringBuilder.append(currentByteString.slice(byteStringStart, byteStringLength - byteStringStart));
    }
    return stringBuilder.toString();
  }

}
