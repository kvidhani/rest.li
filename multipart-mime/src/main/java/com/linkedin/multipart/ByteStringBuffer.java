package com.linkedin.multipart;

import com.linkedin.data.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kvidhani on 8/5/15.
 */
//todo java docs for each method
//nothing is thread safe
//mention that:
//1. Subbuffers only hold bare minimum of byte string references
//2. This super class is the only class that allows mutation.
//3. In this super class byteOffsetEnd will always be the size of the last ByteString since adds only add new
//ByteStrings.
//4. byteOffsetBeginning will vary. it will start with 0 but as trims occur it will change.

class ByteStringBuffer {
  protected List<ByteString> _byteStringList;
  protected int _byteOffsetBeginning; //Offset from first byte string
  protected int _byteOffsetEnd; //offset in last byte string

  public ByteStringBuffer() {
    this(0, 0, new ArrayList<ByteString>());
  }

  protected ByteStringBuffer(final int byteOffsetBeginning, final int byteOffsetEnd,
                             final List<ByteString> byteStringList) {
    if (byteOffsetBeginning < 0 || byteOffsetEnd < 0) {
      throw new IllegalArgumentException("Invalid range specified");
    }
    _byteStringList = byteStringList;
    _byteOffsetBeginning = byteOffsetBeginning;
    _byteOffsetEnd = byteOffsetEnd;
  }

  void add(final ByteString byteString) {
    if (byteString == null) {
      throw new IllegalArgumentException("Null ByteString provided");
    }

    _byteStringList.add(byteString);
    _byteOffsetEnd = byteString.length();
  }

  int size() {
    if (_byteStringList.size() == 0) {
      return 0;
    }

    int totalCount = 0;
    for (int i = 0; i < _byteStringList.size(); i++) {
      totalCount += _byteStringList.get(i).length();
    }

    totalCount -= _byteOffsetBeginning;
    totalCount -= _byteStringList.get(_byteStringList.size() - 1).length() - _byteOffsetEnd;

    return totalCount;
  }

  int indexOfBytes(final byte[] targetBytes)
  {
    if (targetBytes == null) {
      throw new IllegalArgumentException("Target byte array is null");
    }

    if (targetBytes.length == 0)
    {
      return 0;
    }

    outer:
    for (int i = 0; i< size() - targetBytes.length + 1; i++) {
      for (int k = 0; k < targetBytes.length; k++) {
        if (byteAtIndex(i+k) != targetBytes[k]) {
          continue outer;
        }
      }
      return i;
    }
    return -1;
  }

  ByteStringBuffer subBuffer(final int startIndex, final int endIndex, final BufferType bufferType) {
    if (endIndex < startIndex || startIndex < 0 || endIndex < 0) {
      throw new IllegalArgumentException("Invalid range specified");
    }

    int newByteOffsetBeginning = -1;
    int newByteOffsetEnd = -1;
    final List<ByteString> newByteStringList = new ArrayList<ByteString>();

    if (endIndex == startIndex) {
      //Essentially a view in an empty list
      newByteOffsetBeginning = 0;
      newByteOffsetEnd = 0;
    } else {
      newByteStringList.addAll(_byteStringList);
      int currentCount = 0;
      for (int i = 0; i < _byteStringList.size(); i++) {
        final ByteString currentByteString = _byteStringList.get(i);
        //Take into account the offset in the first and final ByteString
        int byteStringStart = i == 0 ? _byteOffsetBeginning : 0;
        int byteStringLength = i == _byteStringList.size() - 1 ? _byteOffsetEnd : currentByteString.length();
        for (int j = byteStringStart; j < byteStringLength; j++) {
          currentCount++;
          if (currentCount == startIndex) {
            for (int k = 0; k < i; k++) {
              newByteStringList.remove(k);
            }
            newByteOffsetBeginning = j;
          }
          if (currentCount == endIndex) {
            for (int k = i; k < _byteStringList.size(); k++) {
              newByteStringList.remove(k);
            }
            newByteOffsetEnd = j;
          }
        }
      }

      //We use these techniques of calculating the limit instead of using size() to save an extra
      //iteration through all of our data.
      if (newByteOffsetBeginning == -1) {
        throw new IllegalArgumentException("Provided start index larger then size of ByteString buffer");
      }

      if (newByteOffsetEnd == -1) {
        throw new IllegalArgumentException("Provided end index larger then size of ByteString buffer");
      }
    }

    if (bufferType == BufferType.IMMUTABLE) {
      return new ImmutableByteStringBuffer(newByteOffsetBeginning, newByteOffsetEnd, _byteStringList);
    } else {
      return new PartData(newByteOffsetBeginning, newByteOffsetEnd, _byteStringList);
    }
  }

  void trimFromBeginning(final int newStartIndex) {

    if (newStartIndex < 0) {
      throw new IllegalArgumentException("Invalid new start index specified");
    }

    //We should always have this invariant true for this class
    assert(_byteOffsetEnd == _byteStringList.get(_byteStringList.size() -1).length());
    int currentCount = 0;
    for (int i = 0; i < _byteStringList.size(); i++) {
      //Take into account the offset in the first and final ByteString
      int byteStringStart = i == 0 ? _byteOffsetBeginning : 0;
      for (int j = byteStringStart; j < _byteStringList.get(i).length(); j++) {
        currentCount++;
        if (currentCount == newStartIndex) {
          //We have found the index in our ByteString buffer where we want to trim up until.
          for (int k = 0; k < i; k++) {
            _byteStringList.remove(k);
          }
          _byteOffsetBeginning = j;
          return;
        }
      }
    }

    //We use this technique of calculating the limit instead of using size() to save an extra
    //iteration through all of our data.
    throw new IllegalArgumentException("Provided new start index larger then size of ByteString buffer");
  }

  byte byteAtIndex(final int index)
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

  enum BufferType {
    IMMUTABLE,
    PART_DATA
  }
}